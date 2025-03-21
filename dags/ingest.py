from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def log_active_gsa():
    import google.auth
    creds, project = google.auth.default()
    gsa = getattr(creds, 'service_account_email', 'No email found')
    logging.info(f"✅ GCP project: {project}")
    logging.info(f"🔥 Active GSA: {gsa}")

with DAG(
    dag_id="debug_gsa_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["debug"],
) as dag:
    log_gsa = PythonOperator(
        task_id="log_active_gsa",
        python_callable=log_active_gsa,
    )
