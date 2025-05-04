from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from data_eng.weather_history import fetch_weather_history

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def fetch_weather_data():
    fetch_weather_history.run()


with DAG(
    dag_id='weather_history_dag',
    default_args=default_args,
    description='Fetch daily weather history for Bangkok',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'bq'],
) as dag:

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_history',
        python_callable=fetch_weather_data
    )

    fetch_weather_task
