from google.cloud import bigquery
from ML.store.big_query import client
import requests
from ML.data_prep.config import Config 
import pandas as pd 

orgs_schema = [
    bigquery.SchemaField("fonduegroup_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("avg_star", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("post_finish_percentage", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("avg_duration_minutes_finished", "FLOAT", mode="REQUIRED"),
]

project_id = "dsde-458712"
dataset_id = "bkk_traffy_fondue"
table_id = "orsgs_profile"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True,
    schema=orgs_schema,
)

def update_orgs():
    conf = Config()
    resp = requests.get(conf.org_url)
    data = resp.json()
    orgs = data['results']
    df = pd.DataFrame(orgs)
    
    target_cols = ['fonduegroup_name', 'avg_star', 'post_finish_percentage', 'avg_duration_minutes_finished']
    numeric_cols = ['avg_star', 'post_finish_percentage', 'avg_duration_minutes_finished']
    df = df[target_cols]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

    sql_query = f"""
    DELETE FROM `{dataset_id}.{table_id}`
    WHERE TRUE
    """
    client.query(sql_query).result()

    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    return job.result()

def get_orgs():
    query = f"""
        SELECT *
        FROM `{dataset_id}.{table_id}`
    """
    return client.query(query).to_dataframe()