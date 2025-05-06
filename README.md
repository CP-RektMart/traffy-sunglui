# Traffy Sunglui

**Kuy** is word I gave to python, the worst language in the world. (PHP is better)

I write this statement because I fuck with python and airflow half a day.

## Getting Started

1. create `service-accounts` folder at `airflow-workflows/` and place credentials file here

```bash
mkdir airflow-workflows/service-accounts
touch airflow-workflows/service-accounts/dsde-458712-ec9f91c0cc0c.json
```

(filename strict case)

2. go to `airflow-workflows/` and docker compose

```bash
docker compose up --build
```

**Note**: make sure your docker set CPU to more than 2 cores and 4GB of RAM

3. Enjoy Fucking shit airflow

## Data Engineering

### Traffy Fondue Data Fetching

Path: `data_eng/traffy_fondue`

Files:

1. `queryclient.py` is big query setup file it need credentials file to auth
2. `import_csv.py` is for import data in csv (`bangkok_traffy.csv`) from DSDE to big query (also create a schema)
3. `fetch_new_traffy.py` is for fetch new data from traffy fondue API

### Weather History Data Fetching

Path: `data_eng/weather_history`

Files:

1. `queryclient.py` is big query setup file it need credentials file to auth
2. `fetch_weather_history.py` is for fetch new data from weather api

\*\*It may not run with standalone file please trigger in airflow because of shit python language
