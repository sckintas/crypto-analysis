import os
import json
import pandas as pd
import io
from datetime import datetime, timedelta
import logging
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retrieve Airflow variables
PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_DATASET_NAME = Variable.get("BQ_DATASET_NAME", default_var='stg_coins_dataset')
BQ_TABLE_NAME = Variable.get("BQ_TABLE_NAME", default_var='bitcoin_history')
BUCKET_NAME = Variable.get("BUCKET_NAME")

# API endpoint
BITCOIN_HISTORY_URL = "https://api.coincap.io/v2/assets/bitcoin/history?interval=m1"

def fetch_and_upload_to_gcs(bucket_name):
    """
    Fetch Bitcoin history from API, convert to Parquet, and upload to GCS.
    Logs the active GSA to confirm Workload Identity.
    """
    from google.auth import default
    creds, project = default()
    logger.info(f"GCP project from creds: {project}")
    logger.info(f"Active GSA: {getattr(creds, 'service_account_email', 'No email found')}")

    try:
        logger.info(f"Using bucket name: {bucket_name}")
        if not bucket_name:
            raise ValueError("Bucket name is not provided or is empty.")

        logger.info("Fetching Bitcoin minutely history data from the API...")
        response = requests.get(BITCOIN_HISTORY_URL)
        response.raise_for_status()
        data = response.json()

        if not data.get("data"):
            raise ValueError("No data returned from the API.")

        df = pd.DataFrame.from_dict(data['data'])
        logger.info(f"DataFrame created with {len(df)} records.")

        current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        object_name = f"raw/bitcoin_history/bitcoin_history_{current_time}.parquet"
        logger.info(f"Preparing to upload file: {object_name}")

        # Convert DataFrame to Parquet in memory
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_file(buffer, timeout=300)

        logger.info(f"Successfully uploaded {object_name} to GCS.")

    except Exception as e:
        logger.error(f"Error in fetch_and_upload_to_gcs: {e}")
        raise

# DAG default args
afw_default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 25),
    "depends_on_past": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="bitcoin_history_dag",
    schedule_interval=timedelta(minutes=1),
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
        source_objects=["raw/bitcoin_history/bitcoin_history_*.parquet"],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        autodetect=True,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
    )

    trigger_dbt_dag_task = TriggerDagRunOperator(
        task_id='trigger_dbt_dag',
        trigger_dag_id='transform_data_in_dbt_dag',
    )

    # Set task dependencies
    fetch_and_upload_task >> load_data_to_bq_task >> trigger_dbt_dag_task
