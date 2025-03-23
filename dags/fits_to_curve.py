""""
Pipeline ETL pour le traitement des données des fits RAW à la courbe de lumière
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 0,
}

dag = DAG(
    'fits_to_curve',
    default_args=default_args,
    description='Pipeline ETL pour le traitement des données',
    schedule=None, # prod : timedelta(days=1),
    catchup=False
)

preprocess_photometric = BashOperator(
    task_id='preprocess_photometric',
    bash_command='python /opt/airflow/scripts/preprocess_photometric.py --start-date {{ ds }}',
    dag=dag,
)

generate_apertures = BashOperator(
    task_id='generate_apertures',
    bash_command='python /opt/airflow/scripts/generate_apertures.py',
    dag=dag,
)

generate_astroseismic_signal = BashOperator(
    task_id='process_to_curated',
    bash_command='python /opt/airflow/scripts/generate_astroseismic_signal.py',
    dag=dag,
)

# Définir l'ordre des tâches
preprocess_photometric >> generate_apertures >> generate_astroseismic_signal