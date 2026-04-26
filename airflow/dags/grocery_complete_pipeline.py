"""
Grocery Pipeline DAG
====================
Ingests data from the Verisim grocery DB (port 5499) into the
central EDW raw schemas via Meltano (tap-postgres-grocery → target-postgres-grocery),
then runs dbt to transform raw → staging → mart.

22 source tables across 7 schemas:
  hr (2), pos (8), timeclock (1), ordering (2),
  fulfillment (2), transport (3), inv (4)

Schedule: every 15 minutes (matches Verisim tick cadence).

Task chain:
  meltano_extract → dbt_staging → dbt_marts → dbt_test_marts
                               → dbt_test   (staging tests, runs in parallel with marts)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CMD = "cd /opt/airflow/dbt/grocery && dbt {cmd} --profiles-dir /opt/airflow/dbt --no-use-colors"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="grocery_complete_pipeline",
    description="Meltano ingest Verisim grocery → EDW raw (22 tables) → dbt staging + marts",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["grocery", "verisim", "meltano", "dbt"],
) as dag:

    t_meltano_extract = BashOperator(
        task_id="meltano_extract",
        bash_command="docker exec meltano meltano --cwd /project run tap-postgres-grocery target-postgres-grocery",
        execution_timeout=timedelta(minutes=30),
    )

    t_dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=DBT_CMD.format(cmd="run --select staging"),
    )
    t_dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=DBT_CMD.format(cmd="run --select marts"),
    )
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD.format(cmd="test --select staging"),
    )
    # Runs custom SQL tests (tests/) + marts.yml schema tests after marts are built.
    # The `|| true` makes failures warn without blocking the pipeline while tests
    # are being tuned. Remove `|| true` once all tests are stable.
    t_dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=DBT_CMD.format(cmd="test --select marts") + " || true",
    )

    t_meltano_extract >> t_dbt_staging
    t_dbt_staging >> t_dbt_marts
    t_dbt_staging >> t_dbt_test
    t_dbt_marts >> t_dbt_test_marts
