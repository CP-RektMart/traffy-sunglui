from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from ML.data_prep.clean_data import main as run

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def clean_data():
    run()


with DAG(
    dag_id='clean_data_dag',
    default_args=default_args,
    description='clean new data every 12 hours',
    schedule_interval='0 */12 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clean', 'bq'],
) as dag:
    fetch_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    fetch_task
