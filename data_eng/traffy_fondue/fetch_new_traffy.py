import requests
from google.cloud import bigquery
import time
import pandas as pd
from data_eng.traffy_fondue.queryclient import client, check_existing_ticket_ids, table_ref, dataset_id, table_id, schema, get_last_existing_timestamp
from data_eng.traffy_fondue.utils import convert_to_utc


def run():
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

    # Get the last timestamp from BigQuery
    last_existing_timestamp = get_last_existing_timestamp()

    if last_existing_timestamp is None:
        print("No data found in BigQuery. Fetching all data from the API.")
    else:
        print(
            f"Last existing timestamp in BigQuery: {last_existing_timestamp}")

    # Convert last_existing_timestamp to UTC for comparison
    last_existing_timestamp = convert_to_utc(
        last_existing_timestamp) if last_existing_timestamp else None

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
            new_data_timestamps = []

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
                        "timestamp": convert_to_utc(properties["timestamp"]) if properties["timestamp"] else None,
                        "state": properties["state"],
                        "star": properties["star"] if properties["star"] is not None else None,
                        "count_reopen": properties["count_reopen"],
                        "last_activity": convert_to_utc(properties["last_activity"]) if properties["last_activity"] else None
                    }
                    new_rows.append(row)
                    new_data_timestamps.append(properties["timestamp"])

            if new_rows:
                # Convert new rows to DataFrame
                new_df = pd.DataFrame(new_rows)

                # Upload the data directly to BigQuery
                job = client.load_table_from_dataframe(
                    new_df, table_ref, job_config=job_config)
                job.result()  # Wait for the job to finish
                print(f"Inserted {len(new_df)} rows.")

                # Get the latest timestamp from the new data
                latest_new_data_timestamp = max(new_data_timestamps)

                # Convert latest_new_data_timestamp to UTC
                latest_new_data_timestamp = convert_to_utc(
                    latest_new_data_timestamp) if latest_new_data_timestamp else None

                # Check if the latest new timestamp is older than or equal to the last existing timestamp
                if latest_new_data_timestamp and last_existing_timestamp and latest_new_data_timestamp <= last_existing_timestamp:
                    print("Detected overlap. Stopping fetch.")
                    break

                # Update the offset for the next batch of data
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


if __name__ == "__main__":
    run()
