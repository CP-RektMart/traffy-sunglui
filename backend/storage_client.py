from google.cloud import storage
from dotenv import load_dotenv


def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    load_dotenv()
    client = storage.Client()

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # Create a blob object from the filepath
    blob = bucket.blob(destination_blob_name)

    # Upload the file to GCS
    blob.upload_from_filename(source_file_path)

    print(f"File {source_file_path} uploaded to {destination_blob_name}.")


def download_from_gcs(bucket_name, blob_name, destination_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.download_to_filename(destination_file_name)
    print(f"Downloaded gs://{bucket_name}/{blob_name} to {destination_file_name}")


def delete_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.delete()
    print(f"Deleted gs://{bucket_name}/{blob_name}")
