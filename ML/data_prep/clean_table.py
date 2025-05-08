from google.cloud import bigquery
from ML.store.big_query import client
from datetime import datetime

clean_schema = [
    bigquery.SchemaField("created_at", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("duration", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("is_rain", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("log_duration", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("until_working_time", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("avg_star", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("post_finish_percentage", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("avg_duration_minutes_finished", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("post_finish_percentage", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("post_finish_percentage", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("ป้าย", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ความสะอาด", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("แสงสว่าง", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("สอบถาม", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ร้องเรียน", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("การเดินทาง", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("จราจร", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ท่อระบายน้ำ", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("สะพาน", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("เสียงรบกวน", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ต้นไม้", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("คนจรจัด", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("คลอง", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ถนน", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("เสนอแนะ", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("กีดขวาง", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("สายไฟ", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("PM", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("น้ำท่วม", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ทางเท้า", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("สัตว์จรจัด", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ความปลอดภัย", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ห้องน้ำ", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ป้ายจราจร", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Others", "INTEGER", mode="REQUIRED")
]

project_id = "dsde-458712"
dataset_id = "bkk_traffy_fondue"
table_id = "cleaned_data"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True,
    schema=clean_schema,
)

def insert_clean(df):
    df['created_at'] = datetime.now()
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    return job.result()

