# fastapi dev backend/main.py
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
import pickle
import numpy as np
from typing import Union
from pydantic import BaseModel
from ML.train.storage_client import download_from_gcs, upload_to_gcs

from fastapi import FastAPI

app = FastAPI()


@app.get("/health")
def test_health():
    return {"health": "OK"}


# predict results from client (body) and return model
# update model from path (param) and data (body) from cron job


class PredictionRequest(BaseModel):
    organizations: list
    types: list


def load_model():
    bucket_name = "model_traffy_fongdue"
    blob_name = "model.pkl"
    destination_blob_name = "model.pkl"

    download_from_gcs(
        bucket_name=bucket_name,
        blob_name=blob_name,
        destination_file_name=destination_blob_name,
    )

    print("Load model DONE!")


def load_org_data():
    load_dotenv()

    # Load credentials from service account JSON
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    credentials = service_account.Credentials.from_service_account_file(creds_path)

    # Set your GCP project ID
    project_id = "dsde-458712"

    # Create BigQuery client using service account
    client = bigquery.Client(credentials=credentials, project=project_id)

    query = "SELECT * FROM `dsde-458712.bkk_traffy_fondue.orsgs_profile`"
    return client.query(query).to_dataframe()


@app.get("/models/predict")
def predict(request: PredictionRequest):
    load_model()

    with open("model.pkl", "rb") as f:
        model = pickle.load(f)

    feature_order = [
        "until_working_time",
        "avg_star",
        "post_finish_percentage",
        "avg_duration_minutes_finished",
        "ป้าย",
        "ความสะอาด",
        "แสงสว่าง",
        "สอบถาม",
        "ร้องเรียน",
        "การเดินทาง",
        "จราจร",
        "ท่อระบายน้ำ",
        "สะพาน",
        "เสียงรบกวน",
        "ต้นไม้",
        "คนจรจัด",
        "คลอง",
        "ถนน",
        "เสนอแนะ",
        "กีดขวาง",
        "สายไฟ",
        "PM2_5",
        "น้ำท่วม",
        "ทางเท้า",
        "สัตว์จรจัด",
        "ความปลอดภัย",
        "ห้องน้ำ",
        "ป้ายจราจร",
        "Others",
    ]

    feature_values = {key: 0 for key in feature_order}

    org_data = load_org_data()

    matched_orgs = org_data[org_data["fonduegroup_name"].isin(request.organizations)]

    if matched_orgs.empty:
        return {"error": "None of the organizations were found in the data."}

    feature_values["avg_star"] = matched_orgs["avg_star"].mean()
    feature_values["post_finish_percentage"] = matched_orgs[
        "post_finish_percentage"
    ].mean()
    feature_values["avg_duration_minutes_finished"] = matched_orgs[
        "avg_duration_minutes_finished"
    ].mean()

    for t in request.types:
        if t in feature_values:
            feature_values[t] = 1

    print(feature_values)

    input_data = [[feature_values[feature] for feature in feature_order]]
    prediction = model.predict(input_data)

    return {"prediction": prediction.tolist()}


class UpdateRequest(BaseModel):
    data: list


@app.put("/models/update")
def update():
    return {"l": "sadsad"}
