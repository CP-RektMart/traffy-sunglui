from google.cloud import bigquery
import pandas as pd
from queryclient import client, table_ref, schema

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True,
    schema=scWhema,
)

chunksize = 100000
for i, chunk in enumerate(pd.read_csv("./data-eng/big-query/bangkok_traffy.csv", chunksize=chunksize)):
    print(f"Uploading chunk {i+1} with {len(chunk)} rows...")
    job = client.load_table_from_dataframe(
        chunk, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Finished chunk {i+1}")
