import os
from google.cloud import bigquery

# Constants
BUCKET_NAME = os.getenv("GCS_BUCKET")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "fintech_dataset"
TABLE_ID = "transactions"
LATEST_FILE_TRACKER = "/opt/airflow/data/processed/latest_file.txt"

def load_to_bq():
    """Loads the recently uploaded CSV file from GCS into BigQuery."""
    if not BUCKET_NAME:
        print("Error: GCS_BUCKET environment variable is not set.")
        return
        
    if not os.path.exists(LATEST_FILE_TRACKER):
        print(f"Error: Could not find the tracker file at {LATEST_FILE_TRACKER}. Did the upload task run?")
        return
        
    # Read the filename that the upload task just created
    with open(LATEST_FILE_TRACKER, "r") as f:
        source_blob_name = f.read().strip()
        
    source_uri = f"gs://{BUCKET_NAME}/{source_blob_name}"
        
    print(f"Loading data from {source_uri} to BigQuery table {DATASET_ID}.{TABLE_ID}...")

    # Initialize client (picks up GOOGLE_APPLICATION_CREDENTIALS automatically)
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        # Now we APPEND because we have historic files!
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(
        source_uri, table_ref, job_config=job_config
    ) 

    load_job.result()  

    destination_table = client.get_table(table_ref)
    print(f"Loaded successfully. Table now has {destination_table.num_rows} total rows.")

if __name__ == "__main__":
    load_to_bq()
