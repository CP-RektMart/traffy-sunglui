from google.cloud import bigquery
from google.oauth2 import service_account

# Load credentials from service account JSON
credentials = service_account.Credentials.from_service_account_file(
    "./data-eng/dsde-458712-ec9f91c0cc0c.json")

# Set your GCP project ID
project_id = "dsde-458712"
dataset_id = "bkk_traffy_fondue"
table_id = "weather_history"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Create BigQuery client using service account
client = bigquery.Client(credentials=credentials, project=project_id)
