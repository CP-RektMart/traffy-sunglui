from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    "./big-query/dsde-458712-ec9f91c0cc0c.json")

project_id = "dsde-458712"

client = bigquery.Client(credentials=credentials, project=project_id)

query = """
    SELECT *
    FROM `dsde-458712.bkk_traffy_fondue.traffy_fondue_data`
    LIMIT 20
"""

df = client.query(query).to_dataframe()

print(df.head())
