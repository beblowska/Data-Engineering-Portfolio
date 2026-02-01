from airflow import DAG
from airflow.operators import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os

def run_pipeline():
    from jobs.data_quality import run_pipeline
    run_pipeline()

with DAG(
    dag_id="data_quality_monitoring",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-quality", "reporting", "csv"]
) as dag:

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=run_pipeline
    )

    generate_report
