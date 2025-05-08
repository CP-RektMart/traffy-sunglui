from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from ML.data_prep.orgs_table import update_orgs

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def run():
    update_orgs()
    print('update orgs successful')

with DAG(
    dag_id='update_orgs_dag',
    default_args=default_args,
    description='update new organization ranking data every 12 hours',
    schedule_interval='0 */12 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['traffy', 'bq'],
) as dag:
    fetch_task = PythonOperator(
        task_id='update_orgs_task',
        python_callable=run
    )

    fetch_task
