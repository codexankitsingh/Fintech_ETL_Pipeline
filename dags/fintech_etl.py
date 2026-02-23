from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fintech_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for Fintech transactions',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['fintech', 'etl'],
) as dag:

    # Task 2: Process Data (Extract & Transform)
    process_data_task = BashOperator(
        task_id='process_data',
        bash_command='python /opt/airflow/scripts/process_data.py',
    )

    # Task 3: Upload to GCS (Load)
    upload_to_gcs_task = BashOperator(
        task_id='upload_to_gcs',
        bash_command='python /opt/airflow/scripts/upload_to_gcs.py',
    )
    
    # Task 4: Load to BigQuery (Warehouse)
    load_to_bq_task = BashOperator(
        task_id='load_to_bq',
        bash_command='python /opt/airflow/scripts/load_to_bq.py',
    )

    # Task Dependencies
    process_data_task >> upload_to_gcs_task >> load_to_bq_task
