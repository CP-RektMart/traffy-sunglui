import numpy as np
import pandas as pd
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from ML.train.config import Config
from ML.utils.logger import log_decorator
from ML.train.storage_client import upload_to_gcs, download_from_gcs
import pickle
import os


@log_decorator
def load_data(path):
    df = pd.read_csv(path)
    return df

@log_decorator
def prep_data(df: pd.DataFrame):
    return df.drop(columns=['duration'])

@log_decorator
def stream_data(df, batch_size):
    for i in range(0, len(df), batch_size):
        yield df.iloc[i:i+batch_size]

@log_decorator
def load_model(cfg: Config):
    download_from_gcs(
        bucket_name="model_traffy_fongdue",
        blob_name="model.pkl",
        destination_file_name="model.pkl"
    )

    with open('model.pkl', 'rb') as f:
        model = pickle.load(f)

    scaler = StandardScaler()
    return model, scaler

@log_decorator
def train_model(model, scaler, df, cfg: Config):
    # For visualization
    y_true_all, y_pred_all = [], []

    # Example data stream
    for batch in stream_data(df, batch_size=cfg.batch_size):
        X = batch.drop(columns=['log_duration'])
        y = batch['log_duration']

        # Normalize input features
        scaler.fit(X)
        model.partial_fit(scaler.transform(X), y)

        y_pred = model.predict(scaler.transform(X))

        y_true_all.extend(y)
        y_pred_all.extend(y_pred)

    y_true_all = np.expm1(y_true_all).tolist()
    y_pred_all = np.expm1(y_pred_all).tolist()

    calculate_loss(y_true_all, y_pred_all)

@log_decorator
def calculate_loss(y_true_all, y_pred_all):
    # Evaluation
    mae = mean_absolute_error(y_true_all, y_pred_all)
    rmse = np.sqrt(mean_squared_error(y_true_all, y_pred_all))
    r2 = r2_score(y_true_all, y_pred_all)

    print(f"MAE: {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")
    print(f"R²: {r2:.2f}")

def main():
    conf = Config()
    df = load_data(conf.load_path)
    df = prep_data(df)
    model, scaler = load_model(conf)
    train_model(model, scaler, df, conf)
    
    model_filename = 'model.pkl'
    with open(model_filename, 'wb') as f:
        pickle.dump(model, f)

    upload_to_gcs(
        bucket_name="model_traffy_fongdue",
        source_file_path=model_filename,
        destination_blob_name=model_filename,        
    )

    os.remove(model_filename)

if __name__ == '__main__':
    main()