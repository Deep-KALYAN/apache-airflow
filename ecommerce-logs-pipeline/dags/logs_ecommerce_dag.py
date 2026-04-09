from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import logging

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'logs_ecommerce_dag',
    default_args=default_args,
    description='ETL pipeline for e-commerce logs with HDFS',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'hdfs', 'logs'],
)

def generer_logs_journaliers(**context):
    execution_date = context["ds"]
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"
    
    cmd = ["python3", script_path, execution_date, "1000", fichier_sortie]
    subprocess.run(cmd, capture_output=True, text=True, check=True)
    
    context['ti'].xcom_push(key='file_path', value=fichier_sortie)
    return fichier_sortie

t_generer = PythonOperator(
    task_id='generer_logs_journaliers',
    python_callable=generer_logs_journaliers,
    dag=dag,
)

# Upload avec follow redirects (-L)
t_upload = BashOperator(
    task_id='uploader_vers_hdfs',
    bash_command="""
        FICHIER_LOCAL="{{ ti.xcom_pull(task_ids='generer_logs_journaliers', key='file_path') }}"
        EXECUTION_DATE="{{ ds }}"
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        
        echo "=== UPLOAD VIA WebHDFS ==="
        echo "Fichier local: ${FICHIER_LOCAL}"
        echo "Destination: ${CHEMIN_HDFS}"
        
        # Upload avec follow redirects
        curl -L -X PUT "http://namenode:9870/webhdfs/v1${CHEMIN_HDFS}?op=CREATE&user.name=root&overwrite=true" \
            -H "Content-Type: application/octet-stream" \
            -T ${FICHIER_LOCAL} \
            -w "\\nHTTP Code: %{http_code}\\n"
        
        rm -f ${FICHIER_LOCAL}
        echo "=== UPLOAD TERMINE ==="
    """,
    dag=dag,
)

t_sensor = BashOperator(
    task_id='hdfs_file_sensor',
    bash_command="""
        EXECUTION_DATE="{{ ds }}"
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        
        echo "=== WAITING FOR FILE ==="
        for i in {1..30}; do
            HTTP_CODE=$(curl -L -s -o /dev/null -w "%{http_code}" \
                "http://namenode:9870/webhdfs/v1${CHEMIN_HDFS}?op=GETFILESTATUS&user.name=root" 2>/dev/null)
            
            echo "Tentative $i/30 - HTTP $HTTP_CODE"
            
            if [ "$HTTP_CODE" = "200" ]; then
                echo "✅ Fichier trouvé après $((i*5)) secondes"
                exit 0
            fi
            sleep 5
        done
        
        echo "❌ Fichier non trouvé après 150 secondes"
        exit 1
    """,
    dag=dag,
)

t_analyser = BashOperator(
    task_id='analyser_logs_hdfs',
    bash_command="""
        EXECUTION_DATE="{{ ds }}"
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        TMP_FILE="/tmp/logs_${EXECUTION_DATE}.txt"
        
        echo "=== ANALYSE DES LOGS ==="
        
        curl -L -s "http://namenode:9870/webhdfs/v1${CHEMIN_HDFS}?op=OPEN&user.name=root" > ${TMP_FILE}
        
        echo ""
        echo "=== CODES HTTP ==="
        grep -oE 'HTTP/1.1" [0-9]{3}' ${TMP_FILE} | awk '{print $2}' | sort | uniq -c | sort -rn
        
        echo ""
        echo "=== TOP 5 URLS ==="
        grep -oE '"(GET|POST) [^"]+' ${TMP_FILE} | awk '{print $2}' | sort | uniq -c | sort -rn | head -5
        
        TOTAL=$(wc -l < ${TMP_FILE})
        ERREURS=$(grep -cE 'HTTP/1.1" [45][0-9]{2}' ${TMP_FILE} || echo 0)
        
        if [ ${TOTAL} -gt 0 ]; then
            TAUX=$(echo "scale=2; ${ERREURS} * 100 / ${TOTAL}" | bc)
        else
            TAUX=0
        fi
        
        echo ""
        echo "=== TAUX D'ERREUR ==="
        echo "Total requêtes: ${TOTAL}"
        echo "Erreurs (4xx/5xx): ${ERREURS}"
        echo "Taux: ${TAUX}%"
        
        # Écrire le taux même s'il est nul
        echo "${TAUX}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
        cat /tmp/taux_erreur_${EXECUTION_DATE}.txt  # Debug
        
        rm ${TMP_FILE}
    """,
    dag=dag,
)

# t_analyser = BashOperator(
#     task_id='analyser_logs_hdfs',
#     bash_command="""
#         EXECUTION_DATE="{{ ds }}"
#         CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
#         TMP_FILE="/tmp/logs_${EXECUTION_DATE}.txt"
        
#         echo "=== ANALYSE DES LOGS ==="
        
#         curl -L -s "http://namenode:9870/webhdfs/v1${CHEMIN_HDFS}?op=OPEN&user.name=root" > ${TMP_FILE}
        
#         echo ""
#         echo "=== CODES HTTP ==="
#         grep -oE 'HTTP/1.1" [0-9]{3}' ${TMP_FILE} | awk '{print $2}' | sort | uniq -c | sort -rn
        
#         echo ""
#         echo "=== TOP 5 URLS ==="
#         grep -oE '"(GET|POST) [^"]+' ${TMP_FILE} | awk '{print $2}' | sort | uniq -c | sort -rn | head -5
        
#         TOTAL=$(wc -l < ${TMP_FILE})
#         ERREURS=$(grep -cE 'HTTP/1.1" [45][0-9]{2}' ${TMP_FILE} || echo 0)
        
#         if [ ${TOTAL} -gt 0 ]; then
#             TAUX=$(echo "scale=2; ${ERREURS} * 100 / ${TOTAL}" | bc)
#         else
#             TAUX=0
#         fi
        
#         echo ""
#         echo "=== TAUX D'ERREUR ==="
#         echo "Total requêtes: ${TOTAL}"
#         echo "Erreurs (4xx/5xx): ${ERREURS}"
#         echo "Taux: ${TAUX}%"
        
#         echo "${TAUX}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
#         rm ${TMP_FILE}
#     """,
#     dag=dag,
# )

SEUIL_ERREUR_PCT = 5.0

def brancher_selon_taux_erreur(**context):
    execution_date = context["ds"]
    try:
        with open(f"/tmp/taux_erreur_{execution_date}.txt", 'r') as f:
            taux = float(f.read().strip())
        logging.info(f"📊 Taux d'erreur: {taux}%")
        
        if taux > SEUIL_ERREUR_PCT:
            logging.warning("⚠️ Alerte OPS")
            return 'alerter_equipe_ops'
        else:
            logging.info("✅ Pipeline OK")
            return 'archiver_rapport_ok'
    except Exception as e:
        logging.error(f"Erreur: {e}")
        return 'archiver_rapport_ok'

t_branch = BranchPythonOperator(
    task_id='brancher_selon_taux_erreur',
    python_callable=brancher_selon_taux_erreur,
    dag=dag,
)

def alerter_equipe_ops(**context):
    logging.error("🚨 ALERTE: Taux d'erreur anormal!")

t_alerte = PythonOperator(
    task_id='alerter_equipe_ops',
    python_callable=alerter_equipe_ops,
    dag=dag,
)

def archiver_rapport_ok(**context):
    logging.info("✅ Pipeline exécuté avec succès")

t_archive_ok = PythonOperator(
    task_id='archiver_rapport_ok',
    python_callable=archiver_rapport_ok,
    dag=dag,
)

t_archiver = BashOperator(
    task_id='archiver_logs_hdfs',
    bash_command="""
        EXECUTION_DATE="{{ ds }}"
        SOURCE="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        DEST="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"
        
        echo "=== ARCHIVAGE ==="
        
        # Lire et copier avec follow redirects
        curl -L -s "http://namenode:9870/webhdfs/v1${SOURCE}?op=OPEN&user.name=root" > /tmp/file.log
        
        # Upload vers processed
        curl -L -X PUT "http://namenode:9870/webhdfs/v1${DEST}?op=CREATE&user.name=root&overwrite=true" \
            -H "Content-Type: application/octet-stream" \
            -T /tmp/file.log 2>/dev/null
        
        # Supprimer source
        curl -L -X DELETE "http://namenode:9870/webhdfs/v1${SOURCE}?op=DELETE&user.name=root&recursive=false" 2>/dev/null
        
        echo "✅ Log archivé"
        rm -f /tmp/file.log
    """,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

t_generer >> t_upload >> t_sensor >> t_analyser >> t_branch
t_branch >> [t_alerte, t_archive_ok] >> t_archiver