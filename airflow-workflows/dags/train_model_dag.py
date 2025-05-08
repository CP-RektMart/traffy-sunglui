from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from ML.train.main import main

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def run():
    main()
    print('train model successful')

with DAG(
    dag_id='train_model_dag',
    default_args=default_args,
    description='train model every 12 hours',
    schedule_interval='0 */12 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['traffy', 'bq'],
) as dag:
    train_model = PythonOperator(
        task_id='train_model_task',
        python_callable=run
    )

    train_model