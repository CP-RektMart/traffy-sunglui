from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from ML.data_prep.clean_data import clean_data
from data_eng.traffy_fondue.fetch_new_traffy import run 
from data_eng.weather_history.fetch_weather_history import run 
from ML.data_prep.orgs_table import update_orgs
from ML.train.train_model import main


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def clean_data_func():
    clean_data()

def fetch_new_traffy_func():
    run()

def fetch_weather_history_func():
    run()

def update_orgs_func():
    update_orgs()

def train_model_func():
    main()



with DAG(
    dag_id='clean_data_dag',
    default_args=default_args,
    description='clean new data every 12 hours',
    schedule_interval='0 */12 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clean', 'bq'],
) as dag:
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data_func
    )

    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_new_traffy_func
    )

    update_orgs_tasks = PythonOperator(
        task_id='update_orgs_task',
        python_callable=update_orgs_func
    )

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_history',
        python_callable=fetch_weather_history_func
    )

    train_model_task = PythonOperator(
        task_id='train_model_task',
        python_callable=train_model_func
    ) 

    fetch_task >> clean_data_task
    update_orgs_tasks >> clean_data_task
    fetch_weather_task >> clean_data_task
    clean_data_task >> train_model_task