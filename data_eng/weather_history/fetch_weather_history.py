import openmeteo_requests
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from data_eng.weather_history.queryclient import client, project_id, dataset_id, table_id, table_ref, schema


def run():
    # Settings
    LATITUDE = 13.7563
    LONGTITUDE = 100.5018

    START_DATE = "2021-08-01"
    END_DATE = datetime.now().strftime("%Y-%m-%d")

    RAIN_SUM_THRESHOLD = 1  # mm
    PRECIPITATION_HOURS_THRESHOLD = 3  # hours

    openmeteo = openmeteo_requests.Client()

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGTITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": ["rain_sum", "precipitation_hours", "precipitation_sum"],
        "timezone": "Asia/Bangkok",
    }
    responses = openmeteo.weather_api(url, params=params)

    response = responses[0]
    print(f"Getting rain data for Bangkok at {response.Latitude():.2f}°N, {response.Longitude():.2f}°E "
          f"(Elevation: {response.Elevation():.0f} m asl, Timezone: {response.Timezone().decode()} {response.TimezoneAbbreviation().decode()}, "
          f"UTC offset: {response.UtcOffsetSeconds()}s)")

    daily = response.Daily()
    daily_rain_sum = daily.Variables(0).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(1).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(2).ValuesAsNumpy()

    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left",
        )
    }

    daily_data["rain_sum"] = daily_rain_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["precipitation_sum"] = daily_precipitation_sum

    rain_df = pd.DataFrame(data=daily_data)

    rain_df['is_rain'] = (rain_df['rain_sum'] >= RAIN_SUM_THRESHOLD) & (
        rain_df['precipitation_hours'] >= PRECIPITATION_HOURS_THRESHOLD)

    rain_df['date'] = pd.to_datetime(rain_df["date"]).dt.date

    # === Query max date in BigQuery ===
    try:
        query = f"""
        SELECT MAX(date) AS max_date
        FROM `{project_id}.{dataset_id}.{table_id}`
        """
        max_date_result = client.query(query).result()
        max_date_row = next(max_date_result, None)
        max_date = max_date_row.max_date if max_date_row else None
    except Exception as e:
        print(f"Error querying max date: {e}")
        max_date = None

    # === Filter rain_df based on max_date ===
    if max_date:
        rain_df = rain_df[rain_df["date"] > max_date]
        print(
            f"Filtered data to dates after {max_date}. Remaining rows: {len(rain_df)}")
    else:
        print("Table is empty or not found. Appending all rows.")

    if rain_df.empty:
        print("No new data to insert.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema
    )

    # Upload to BigQuery
    job = client.load_table_from_dataframe(
        rain_df,
        table_ref,
        job_config=job_config
    )
    job.result()  # Waits for the job to complete
    print(f"Inserted {len(rain_df)} new rows of rain data into BigQuery.")


if __name__ == "__main__":
    run()
