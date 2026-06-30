"""
Grocery Complete Pipeline DAG
==============================
Runs the full grocery pipeline end-to-end:
  1. grocery_ingest_api  — load all 27 source tables into raw_* schemas
  2. grocery_dbt         — transform raw → staging → marts
  3. grocery_freshness   — check source freshness (via grocery_dbt DAG)

Each child DAG runs to completion before the next starts.
Schedule: every 6 hours (00:00, 06:00, 12:00, 18:00 ET).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="grocery_complete_pipeline",
    description="Full grocery pipeline: API ingest → dbt transform",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["grocery", "pipeline"],
) as dag:

    ingest = TriggerDagRunOperator(
        task_id="ingest",
        trigger_dag_id="grocery_ingest_api",
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(hours=2),
    )

    transform = TriggerDagRunOperator(
        task_id="transform",
        trigger_dag_id="grocery_dbt",
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(hours=1),
    )

    ingest >> transform
