from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A test DAG',
    schedule=timedelta(days=1),
)

# Tâche 1: Extraction vers raw
hello_task = BashOperator(
    task_id='hello_task',
    bash_command='python /opt/airflow/scripts/hello.py',
    dag=dag,
)


# Définir l'ordre des tâches
hello_task