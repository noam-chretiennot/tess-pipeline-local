"""
This DAG downloads the data from the TESS pipeline and puts it in the datalake.

NOTE: The download data is saved to disk twice.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'unpack_to_api',
    default_args=default_args,
    description='Download data and put it in the datalake, failsafe',
    schedule_interval=None,
    catchup=False
)

unpack_to_api = BashOperator(
    task_id='unpack_to_api',
    bash_command='python /opt/airflow/build/unpack_to_api.py',
    dag=dag,
)

unpack_to_api
