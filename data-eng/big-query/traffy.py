import requests
import json

# URL of the API endpoint
url = "https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1?limit=1000&offset=1000"

# Send GET request to the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the response JSON into a Python dictionary
    data = response.json()

    # Print the data (GeoJSON)
    rows_to_insert = []
    for feature in data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]

        # Map the JSON data to the BigQuery schema
        row = {
            "ticket_id": properties["ticket_id"],
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

        json_formatted_str = json.dumps(row, indent=2, ensure_ascii=False)

        # Print the formatted JSON string
        print(json_formatted_str.encode('utf-8').decode('utf-8'))
        print("===================================")

        # Append the row to the list of rows to insert
        rows_to_insert.append(row)
else:
    print(f"Failed to fetch data. HTTP Status code: {response.status_code}")
