from google.cloud import bigquery
from google.oauth2 import service_account

# Load credentials from service account JSON
credentials = service_account.Credentials.from_service_account_file(
    "./data-eng/big-query/dsde-458712-ec9f91c0cc0c.json")

# Set your GCP project ID
project_id = "dsde-458712"
dataset_id = "bkk_traffy_fondue"
table_id = "traffy_fondue_data"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Create BigQuery client using service account
client = bigquery.Client(credentials=credentials, project=project_id)

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


def check_existing_ticket_ids(dataset_id, table_id, ticket_ids):
    query = f"""
    SELECT ticket_id
    FROM `{dataset_id}.{table_id}`
    WHERE ticket_id IN UNNEST(@ticket_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("ticket_ids", "STRING", ticket_ids)
        ]
    )
    query_job = client.query(query, job_config=job_config)

    # Wait for query results
    result = query_job.result()
    existing_ticket_ids = {row["ticket_id"] for row in result}
    return existing_ticket_ids


def get_last_existing_timestamp():
    query = f"""
    SELECT MAX(timestamp) AS last_timestamp
    FROM `{dataset_id}.{table_id}`;
    """
    result = client.query(query).result()
    for row in result:
        return row["last_timestamp"]
    return None
