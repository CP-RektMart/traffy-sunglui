from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Load credentials from service account JSON
creds_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
credentials = service_account.Credentials.from_service_account_file(creds_path)

# Set your GCP project ID
project_id = "dsde-458712"
dataset_id = "bkk_traffy_fondue"
table_id = "weather_history"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Create BigQuery client using service account
client = bigquery.Client(credentials=credentials, project=project_id)

schema = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("rain_sum", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("precipitation_hours", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("precipitation_sum", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("is_rain", "BOOLEAN"),
]
