from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'test',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

test_task = BashOperator(
    task_id='test_task',
    bash_command='echo "Airflow is working!" && date',
    dag=dag,
)
