from queryclient import client, get_last_existing_timestamp
import pandas as pd
from utils import convert_to_utc

# query = """
#     DELETE FROM `dsde-458712.bkk_traffy_fondue.traffy_fondue_data`
#     WHERE timestamp IN (
#     SELECT timestamp
#     FROM `dsde-458712.bkk_traffy_fondue.traffy_fondue_data`
#     ORDER BY timestamp DESC
#     LIMIT 2000
#     );
# """

# client.query(query)

# print(get_last_existing_timestamp())

# query = """
#     SELECT *
#     FROM `dsde-458712.bkk_traffy_fondue.traffy_fondue_data`
#     ORDER BY timestamp DESC
#     LIMIT 20
#     OFFSET 132218
# """

# df = client.query(query).to_dataframe()

query = """
    SELECT *
    FROM `dsde-458712.bkk_traffy_fondue.traffy_fondue_data`
    ORDER BY timestamp DESC
    LIMIT 20
    OFFSET 0
"""

df = client.query(query).to_dataframe()

print(df[['ticket_id', 'comment', 'timestamp']])


# df = pd.read_csv("./data-eng/big-query/bangkok_traffy.csv")
# print(df.count())

# # Test the conversion
# timestamp_thai = "2025-01-16 06:08:25"
# timestamp_thai = "2025-01-16 02:41:14.080731+00"
# converted_timestamp = convert_to_utc(timestamp_thai)
# print(f"Converted timestamp: {converted_timestamp}")
