"""
A simple Airflow DAG that runs a Python script.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    schedule_interval=timedelta(days=1),  # Fixed incorrect argument name
)

# Task: Run a Python script
hello_task = BashOperator(
    task_id='hello_task',
    bash_command='python /opt/airflow/scripts/hello.py',
    dag=dag,
)

# Define task dependencies
hello_task
