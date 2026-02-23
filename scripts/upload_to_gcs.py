import os
from datetime import datetime
from google.cloud import storage

# Constants
# We use the environment variable if set, otherwise default (good for local testing matches docker params)
BUCKET_NAME = os.getenv("GCS_BUCKET")
SOURCE_FILE = "/opt/airflow/data/processed/processed_transactions.csv"

# File to communicate the dynamic name to the next task
LATEST_FILE_TRACKER = "/opt/airflow/data/processed/latest_file.txt"

def upload_to_gcs():
    """Uploads a file to the bucket with a timestamp."""
    if not BUCKET_NAME:
        raise ValueError("Error: GCS_BUCKET environment variable is not set.")

    if not os.path.exists(SOURCE_FILE):
        print(f"Error: Source file {SOURCE_FILE} not found.")
        return

    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination_blob_name = f"processed_transactions_{timestamp}.csv"

    print(f"Uploading {SOURCE_FILE} to GCS bucket: {BUCKET_NAME} as {destination_blob_name}...")
    
    # Initialize client (picks up GOOGLE_APPLICATION_CREDENTIALS automatically)
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(SOURCE_FILE)

    print(f"File uploaded to gs://{BUCKET_NAME}/{destination_blob_name}")
    
    # Save exactly what we called it so the BigQuery task knows
    with open(LATEST_FILE_TRACKER, "w") as f:
        f.write(destination_blob_name)

if __name__ == "__main__":
    upload_to_gcs()
