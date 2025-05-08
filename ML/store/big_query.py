from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

load_dotenv()

# Load credentials from service account JSON
creds_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
credentials = service_account.Credentials.from_service_account_file(creds_path)


# Set your GCP project ID
project_id = "dsde-458712"

# Create BigQuery client using service account
client = bigquery.Client(credentials=credentials, project=project_id)