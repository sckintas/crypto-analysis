import os
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environmental variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
BQ_DATASET_NAME = os.environ.get("BQ_DATASET_NAME", 'stg_coins_dataset')
BQ_TABLE_NAME = "bitcoin_history"

# API endpoint for Bitcoin minutely history
BITCOIN_HISTORY_URL = "https://api.coincap.io/v2/assets/bitcoin/history?interval=m1"

def fetch_and_upload_to_gcs(bucket_name):
    """
    Fetch minutely Bitcoin history data from the API, convert it to Parquet, and upload it directly to GCS.
    """
    try:
        # Fetch data from the API
        logger.info("Fetching Bitcoin minutely history data from the API...")
        response = requests.get(BITCOIN_HISTORY_URL)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()

        # Convert JSON data to a DataFrame
        df = pd.DataFrame.from_dict(data['data'])

        # Add a timestamp for the filename
        current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        object_name = f"raw/bitcoin_history/bitcoin_history_{current_time}.parquet"

        # Convert DataFrame to Parquet format in memory
        parquet_buffer = df.to_parquet(index=False)

        # Upload the Parquet file directly to GCS
        logger.info("Uploading Parquet file to GCS...")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_string(parquet_buffer, timeout=300)
        logger.info(f"Successfully uploaded {object_name} to GCS.")

    except Exception as e:
        logger.error(f"Error in fetch_and_upload_to_gcs: {e}")
        raise

# Set default arguments
afw_default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 25),
    "depends_on_past": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG declaration
with DAG(
    dag_id="bitcoin_history_dag",
    schedule_interval=timedelta(minutes=1),  # Run every minute
    default_args=afw_default_args,
    max_active_runs=1,
    catchup=False,
    tags=['crypto-analytics-afw'],
) as dag:

    fetch_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_to_gcs",
        python_callable=fetch_and_upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
        },
    )

    load_data_to_bq_task = GCSToBigQueryOperator(
        task_id='load_data_to_bq',
        bucket=BUCKET_NAME,
        source_objects=["raw/bitcoin_history/bitcoin_history_*.parquet"],  # Use a wildcard to match all files
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        autodetect=True,
        write_disposition='WRITE_APPEND',  # Append new data instead of overwriting
        create_disposition='CREATE_IF_NEEDED',
    )

    trigger_dbt_dag_task = TriggerDagRunOperator(
        task_id='trigger_dbt_dag',
        trigger_dag_id='transform_data_in_dbt_dag',
    )

    # Task dependencies
    fetch_and_upload_task >> load_data_to_bq_task >> trigger_dbt_dag_task