# fastapi dev backend/main.py
from typing import Union
from pydantic import BaseModel

from fastapi import FastAPI

app = FastAPI()


@app.get("/health")
def test_health():
    return {"health": "OK"}


# predict results from client (body) and return model
# update model from path (param) and data (body) from cron job


class ModelRequest(BaseModel):
    text: str


@app.get("/models/predict")
def predict(request: ModelRequest):
    # load model from google cloud storage
    body = f"Received text: {request.text}"
    return {"body": body}


class UpdateRequest(BaseModel):
    data: list


@app.put("/models/update")
def update():
    return {"l": "sadsad"}
