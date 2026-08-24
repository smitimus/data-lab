"""
exec_report_s3_export_dag.py — daily export of the Superset exec dashboard
("Store — Executive Performance Overview", resolved by title — ids drift
across reseeds; was id 13, now id 10) to the MinIO S3 bucket.

Part of data-lab#35 (pivot from email to S3) / data-lab#36 (MinIO stack).
The heavy lifting lives in export_exec_report.export().
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from export_exec_report import export

DEFAULT_ARGS = {
    "owner": "infra-dev",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="exec_report_s3_export",
    description="Daily CSV export of Superset exec dashboard (Store — Executive Performance Overview) to MinIO S3 (reports/).",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",          # 06:00 daily
    start_date=datetime(2026, 7, 20),
    catchup=False,
    tags=["reporting", "s3", "superset"],
) as dag:
    export_task = PythonOperator(
        task_id="export_exec_dashboard_to_s3",
        python_callable=export,
    )
