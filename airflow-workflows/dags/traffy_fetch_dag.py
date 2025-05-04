from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from data_eng.traffy_fondue import fetch_new_traffy

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def fetch_traffy_data():
    fetch_new_traffy.run()


with DAG(
    dag_id='traffy_fetch_dag',
    default_args=default_args,
    description='Fetch new Traffy data every 12 hours',
    schedule_interval='0 */12 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['traffy', 'bq'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_traffy_data
    )

    fetch_task
