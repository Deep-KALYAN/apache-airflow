from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Fonction pour la tâche 2
def get_current_date():
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Date du jour : {current_date}")
    return current_date

# Arguments par défaut
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Définition du DAG - Note: 'schedule' au lieu de 'schedule_interval'
with DAG(
    dag_id='exercice_jour1',
    default_args=default_args,
    description='Exercice Airflow - DAG avec 3 tâches',
    schedule=None,  # Changement clé : schedule au lieu de schedule_interval
    catchup=False,
    tags=['exercice', 'jour1'],
) as dag:
    
    # Tâche 1 : Affiche "Début du workflow" (BashOperator)
    tache1_debut = BashOperator(
        task_id='debut_workflow',
        bash_command='echo "Début du workflow"',
    )
    
    # Tâche 2 : Retourne la date du jour (PythonOperator)
    tache2_date = PythonOperator(
        task_id='date_du_jour',
        python_callable=get_current_date,
    )
    
    # Tâche 3 : Affiche "Fin du workflow" (BashOperator)
    tache3_fin = BashOperator(
        task_id='fin_workflow',
        bash_command='echo "Fin du workflow"',
    )
    
    # Définition de l'ordre d'exécution
    tache1_debut >> tache2_date >> tache3_fin