import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Setup logging
logger = logging.getLogger(__name__)

# Airflow variables
PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_DATASET_NAME = Variable.get("BQ_DATASET_NAME", default_var='stg_coins_dataset')
BQ_TABLE_NAME = Variable.get("BQ_TABLE_NAME", default_var='bitcoin_history')
BUCKET_NAME = Variable.get("BUCKET_NAME")

# TEMPORARY FUNCTION to only log active GSA
def fetch_and_upload_to_gcs(bucket_name):
    from google.auth import default
    creds, project = default()
    logger.info(f"GCP project from creds: {project}")
    logger.info(f"Active GSA: {getattr(creds, 'service_account_email', 'No email found')}")
    return  # Skip the rest for now

# DAG default args
afw_default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 25),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="bitcoin_history_dag",
    schedule_interval=timedelta(minutes=1),
    default_args=afw_default_args,
    max_active_runs=1,
    catchup=False,
    tags=['crypto-analytics-afw', 'debug'],
) as dag:

    fetch_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_to_gcs",
        python_callable=fetch_and_upload_to_gcs,
        op_kwargs={"bucket_name": BUCKET_NAME},
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

    fetch_and_upload_task >> load_data_to_bq_task >> trigger_dbt_dag_task
