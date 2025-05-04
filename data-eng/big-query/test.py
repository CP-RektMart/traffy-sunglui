from queryclient import client
import pandas as pd

query = """
    SELECT *
    FROM `dsde-458712.bkk_traffy_fondue.traffy_fondue_data`
    ORDER BY timestamp DESC
    LIMIT 20
    OFFSET 1000
"""

df = client.query(query).to_dataframe()

print(df[['ticket_id', 'comment', 'timestamp']])


# df = pd.read_csv("./data-eng/big-query/bangkok_traffy.csv")
# print(df.count())
