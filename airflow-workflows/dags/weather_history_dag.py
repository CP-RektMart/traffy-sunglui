from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the weather-history folder to sys.path
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../../data-eng/weather-history')))

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def fetch_weather_data():
    import fetch_weather_history
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
