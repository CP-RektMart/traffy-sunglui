from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Load credentials from service account JSON
credentials = service_account.Credentials.from_service_account_file(
    "./big-query/dsde-458712-ec9f91c0cc0c.json")

# Set your GCP project ID
project_id = "dsde-458712"

# Create BigQuery client using service account
client = bigquery.Client(credentials=credentials, project=project_id)

table_id = "dsde-458712.bkk_traffy_fondue.traffy_fondue_data"

schema = [
    bigquery.SchemaField("ticket_id", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("organization", "STRING"),
    bigquery.SchemaField("comment", "STRING"),
    bigquery.SchemaField("photo", "STRING"),
    bigquery.SchemaField("photo_after", "STRING"),
    bigquery.SchemaField("coords", "STRING"),
    bigquery.SchemaField("address", "STRING"),
    bigquery.SchemaField("subdistrict", "STRING"),
    bigquery.SchemaField("district", "STRING"),
    bigquery.SchemaField("province", "STRING"),
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("star", "FLOAT"),
    bigquery.SchemaField("count_reopen", "INTEGER"),
    bigquery.SchemaField("last_activity", "STRING")
]

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True,
    schema=schema,
)

chunksize = 100000
for i, chunk in enumerate(pd.read_csv("./big-query/bangkok_traffy.csv", chunksize=chunksize)):
    print(f"Uploading chunk {i+1} with {len(chunk)} rows...")
    job = client.load_table_from_dataframe(
        chunk, table_id, job_config=job_config)
    job.result()
    print(f"✅ Finished chunk {i+1}")
