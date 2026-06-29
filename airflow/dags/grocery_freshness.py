"""
Grocery Source Freshness DAG
============================
Runs `dbt source freshness` on all 27 grocery source tables and reports
the results. This is a monitoring DAG — it does not ingest or transform data.

Use it to alert when raw source data has gone stale (warn: 24h, error: 48h).

Schedule: every 6 hours (adjust as needed for your operational tempo).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_FRESHNESS = (
    "cd /opt/airflow/dbt/grocery && "
    "dbt source freshness --profiles-dir /opt/airflow/dbt --no-use-colors"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="grocery_freshness",
    description="Monitor dbt source freshness for all 27 grocery raw tables",
    default_args=default_args,
    start_date=datetime(2026, 6, 29),
    schedule="0 */6 * * *",        # every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=["grocery", "monitoring", "freshness"],
) as dag:

    check_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=DBT_FRESHNESS,
        execution_timeout=timedelta(minutes=5),
    )

    check_freshness
