from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from datetime import datetime, timedelta
import subprocess
import logging
import os

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
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logging.info(f"Logs generated: {result.stdout}")
        file_size = os.path.getsize(fichier_sortie)
        logging.info(f"File size: {file_size} bytes")
        return fichier_sortie
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to generate logs: {e.stderr}")
        raise

t_generer = PythonOperator(
    task_id='generer_logs_journaliers',
    python_callable=generer_logs_journaliers,
    dag=dag,
)

t_upload = BashOperator(
    task_id='uploader_vers_hdfs',
    bash_command="""
        EXECUTION_DATE="{{ ds }}"
        FICHIER_LOCAL="/tmp/access_${EXECUTION_DATE}.log"
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        
        echo "Uploading ${FICHIER_LOCAL} to HDFS: ${CHEMIN_HDFS}"
        
        docker cp ${FICHIER_LOCAL} namenode:/tmp/access_${EXECUTION_DATE}.log
        docker exec namenode hdfs dfs -put -f /tmp/access_${EXECUTION_DATE}.log ${CHEMIN_HDFS}
        docker exec namenode rm /tmp/access_${EXECUTION_DATE}.log
        
        echo "Upload completed"
    """,
    dag=dag,
)

t_sensor = HdfsSensor(
    task_id='hdfs_file_sensor',
    filepath="/data/ecommerce/logs/raw/access_{{ ds }}.log",
    hdfs_conn_id="hdfs_default",
    poke_interval=30,
    timeout=300,
    mode="poke",
    dag=dag,
)

t_analyser = BashOperator(
    task_id='analyser_logs_hdfs',
    bash_command="""
        EXECUTION_DATE="{{ ds }}"
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        TMP_FILE="/tmp/logs_analyse_${EXECUTION_DATE}.txt"
        
        echo "Reading file from HDFS: ${CHEMIN_HDFS}"
        
        docker exec namenode hdfs dfs -cat ${CHEMIN_HDFS} > ${TMP_FILE}
        
        echo ""
        echo "=== STATUS CODES DISTRIBUTION ==="
        grep -oP '"[A-Z]+ [^ ]+ HTTP/1.1" \\K[0-9]{3}' ${TMP_FILE} | sort | uniq -c | sort -rn
        
        echo ""
        echo "=== TOP 10 URLs ==="
        grep -oP '"(GET|POST) \\K[^"]+' ${TMP_FILE} | cut -d' ' -f1 | sort | uniq -c | sort -rn | head -10
        
        echo ""
        echo "=== ERROR RATE ==="
        TOTAL=$(wc -l < ${TMP_FILE})
        ERREURS=$(grep -cP '"[A-Z]+ [^ ]+ HTTP/1.1" [45][0-9]{2}' ${TMP_FILE} || echo 0)
        TAUX=$(echo "scale=2; ${ERREURS} * 100 / ${TOTAL}" | bc)
        
        echo "Total requests: ${TOTAL}"
        echo "Errors (4xx/5xx): ${ERREURS}"
        echo "Error rate: ${TAUX}%"
        
        echo "${ERREURS} ${TOTAL} ${TAUX}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
        
        rm ${TMP_FILE}
    """,
    dag=dag,
)

SEUIL_ERREUR_PCT = 5.0

def brancher_selon_taux_erreur(**context):
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
    
    try:
        with open(fichier_taux, 'r') as f:
            erreurs, total, taux = f.read().strip().split()
            taux = float(taux)
        
        logging.info(f"Error rate: {taux}% (threshold: {SEUIL_ERREUR_PCT}%)")
        
        if taux > SEUIL_ERREUR_PCT:
            logging.warning(f"Error rate {taux}% exceeds threshold! Alerting Ops team.")
            return 'alerter_equipe_ops'
        else:
            logging.info(f"Error rate {taux}% is normal. Archiving report.")
            return 'archiver_rapport_ok'
    except Exception as e:
        logging.error(f"Failed to read error rate: {e}")
        return 'alerter_equipe_ops'

t_branch = BranchPythonOperator(
    task_id='brancher_selon_taux_erreur',
    python_callable=brancher_selon_taux_erreur,
    dag=dag,
)

def alerter_equipe_ops(**context):
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
    
    with open(fichier_taux, 'r') as f:
        erreurs, total, taux = f.read().strip().split()
    
    alert_message = f"""
    HIGH ERROR RATE ALERT
    Date: {execution_date}
    Error Rate: {taux}%
    Total Errors: {erreurs} / {total}
    Threshold: {SEUIL_ERREUR_PCT}%
    
    Action Required: Check web servers immediately!
    """
    
    logging.error(alert_message)
    
    with open(f"/tmp/alert_{execution_date}.txt", "w") as f:
        f.write(alert_message)

t_alerte = PythonOperator(
    task_id='alerter_equipe_ops',
    python_callable=alerter_equipe_ops,
    dag=dag,
)

def archiver_rapport_ok(**context):
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
    
    with open(fichier_taux, 'r') as f:
        erreurs, total, taux = f.read().strip().split()
    
    report = f"""
    HEALTHY PIPELINE REPORT
    Date: {execution_date}
    Error Rate: {taux}%
    Total Errors: {erreurs} / {total}
    Status: NORMAL
    """
    
    logging.info(report)
    
    with open(f"/tmp/report_{execution_date}.txt", "w") as f:
        f.write(report)

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
        DESTINATION="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"
        
        echo "Archiving: ${SOURCE} -> ${DESTINATION}"
        
        docker exec namenode hdfs dfs -mv ${SOURCE} ${DESTINATION}
        
        echo "Log archived in processed zone"
    """,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

t_generer >> t_upload >> t_sensor >> t_analyser >> t_branch
t_branch >> [t_alerte, t_archive_ok]
[t_alerte, t_archive_ok] >> t_archiver
