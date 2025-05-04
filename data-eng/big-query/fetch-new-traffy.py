import requests
from google.cloud import bigquery
import time
import pandas as pd
from queryclient import client, check_existing_ticket_ids, table_ref, dataset_id, table_id, schema

# URL of the API endpoint
url = "https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1?limit=1000&offset={offset}"

# Rate limit: 100 requests per minute (0.6 seconds per request)
rate_limit_delay = 60 / 100

# Offset for pagination
offset = 0
has_new_data = True

# Job configuration for BigQuery load
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True,
    schema=schema,
)

while has_new_data:
    # Send GET request to the API
    response = requests.get(url.format(offset=offset))

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response JSON into a Python dictionary
        data = response.json()

        # Extract ticket_ids from the fetched data
        ticket_ids = [feature["properties"]["ticket_id"]
                      for feature in data["features"]]

        # Check if any of the fetched ticket_ids already exist in BigQuery
        existing_ticket_ids = check_existing_ticket_ids(
            dataset_id, table_id, ticket_ids)

        # Filter out existing data and prepare the rows for BigQuery
        new_rows = []
        for feature in data["features"]:
            ticket_id = feature["properties"]["ticket_id"]
            if ticket_id not in existing_ticket_ids:
                # Prepare the row for BigQuery
                properties = feature["properties"]
                geometry = feature["geometry"]
                row = {
                    "ticket_id": ticket_id,
                    "type": "{" + ", ".join([org.strip() for org in properties["org"]]).replace(" ", "") + "}",
                    "organization": ", ".join(properties["org"]),
                    "comment": properties["description"],
                    "photo": properties["photo_url"],
                    "photo_after": properties["after_photo"],
                    "coords": f"{geometry['coordinates'][1]},{geometry['coordinates'][0]}",
                    "address": properties["address"],
                    "subdistrict": properties["subdistrict"],
                    "district": properties["district"],
                    "province": properties["province"],
                    "timestamp": properties["timestamp"],
                    "state": properties["state"],
                    "star": properties["star"] if properties["star"] is not None else None,
                    "count_reopen": properties["count_reopen"],
                    "last_activity": properties["last_activity"]
                }
                new_rows.append(row)

        if new_rows:
            # Convert new rows to DataFrame
            new_df = pd.DataFrame(new_rows)

            # Upload the data directly to BigQuery
            job = client.load_table_from_dataframe(
                new_df, table_ref, job_config=job_config)
            job.result()  # Wait for the job to finish
            print(f"Inserted {len(new_df)} rows.")

            # Check if we have any new data for the next iteration
            has_new_data = len(new_rows) == 1000
            if has_new_data:
                offset += 1000
        else:
            # No new data, stop fetching
            has_new_data = False

    else:
        print(
            f"Failed to fetch data. HTTP Status code: {response.status_code}")
        break

    # Sleep to respect the rate limit (100 requests per minute)
    time.sleep(rate_limit_delay)
