# Traffy Fondue Data Lake

This module is to fetch a newer data from traffy-fondue by compare last timestamp and new data timestamp to determine the new data.

Getting Started

1. place file `credentials.json` (Google IAM Account) at `./data-eng/big-query` and rename file in `queryclient.py`
2. run it

Files:

1. `queryclient.py` is big query setup file it need credentials file to auth
2. `import-csv.py` is for import data in csv (`bangkok_traffy.csv`) from DSDE to big query (also create a schema)
3. `fetch-new-traffy.py` is for fetch new data from traffy fondue API

Need to know:

- Rate Limit in traffy fondue API is `100 req/min`
- timestamp and last_activity in csv is GMT+00 time but data from traffy fondue API is GMT+07. It need to convert.
