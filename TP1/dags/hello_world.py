from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='hello_world_dag',
    description='Mon premier DAG Hello World',
    schedule='@daily',  # Note: 'schedule' pas 'schedule_interval'
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'hello_world'],
) as dag:
    
    hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Airflow!"',
    )



# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator

# # Arguments par défaut
# default_args = {
#     'owner': 'airflow',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def print_hello():
#     print('Hello from Airflow!')
# # ----------------------------------------------------
# # dags/hello_world.py
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator

# # 1. Arguments par défaut appliqués à toutes les tâches
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # 2. Définition du DAG avec context manager
# with DAG(
#     dag_id='hello_world_dag',
#     default_args=default_args,
#     description='Mon premier DAG Hello World',
#     schedule_interval='@daily',  # S'exécute une fois par jour
#     start_date=datetime(2024, 1, 1),
#     catchup=False,  # Ne pas rattraper les exécutions passées
#     tags=['tutorial', 'hello_world'],
# ) as dag:

#     # 3. Déclaration des Tâches (Tasks)
    
#     # Tâche 1 : Afficher "Hello" avec BashOperator
#     task_hello = BashOperator(
#         task_id='say_hello',
#         bash_command='echo "Hello from Airflow!"',
#     )
    
#     # Tâche 2 : Afficher la date avec BashOperator
#     task_date = BashOperator(
#         task_id='show_date',
#         bash_command='echo "Current date is: $(date)"',
#     )
    
#     # Tâche 3 : Fonction Python personnalisée
#     def print_world():
#         print("World! This is from PythonOperator")
#         print(f"Execution date: {{ ds }}")  # ds est une variable Airflow
    
#     task_world = PythonOperator(
#         task_id='say_world',
#         python_callable=print_world,
#     )
    
#     # 4. Définition des dépendances (ordre d'exécution)
#     task_hello >> task_date >> task_world
#     # Ceci signifie: task_hello d'abord, puis task_date, puis task_world