"""
Gas Station Pipeline DAG
========================
Ingests data from the Verisim PostgreSQL source (port 5499) into the
central EDW raw schema via Meltano (Singer tap-postgres → target-postgres),
then runs dbt to transform raw → staging → mart.

Phase 1: Custom Python extraction (7 tables, manual watermarks)
Phase 2: Meltano ingestion layer (16 tables, Singer state management)

Schedule: every 15 minutes (matches Verisim tick cadence).
Can also be triggered manually for backfills.

Tasks:
  meltano_extract — Singer tap-postgres → target-postgres (all 16 Verisim tables)
  dbt_staging     — dbt run --select staging
  dbt_marts       — dbt run --select marts
  dbt_test        — dbt test
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CMD = "cd /opt/airflow/dbt/gasstation && dbt {cmd} --profiles-dir /opt/airflow/dbt --no-use-colors"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="gasstation_pipeline",
    description="Meltano ingest Verisim → EDW raw (16 tables) → dbt staging + marts",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",   # every 15 minutes
    catchup=False,
    max_active_runs=1,
    tags=["gasstation", "verisim", "meltano", "dbt"],
) as dag:

    # -- Meltano extraction (Singer tap-postgres → target-postgres) -----------
    # Replaces the 7-task custom Python extraction from Phase 1.
    # Meltano manages incremental state (bookmarks) internally in
    # /opt/conf/meltano/.meltano/run/ — no _watermarks table needed.
    # NOTE: Docker socket (/var/run/docker.sock) must be mounted in Airflow
    # workers for docker exec to work (added to compose.yaml in Phase 2).
    t_meltano_extract = BashOperator(
        task_id="meltano_extract",
        bash_command="docker exec meltano meltano --cwd /project run tap-postgres target-postgres",
        execution_timeout=timedelta(minutes=30),
    )

    # -- dbt transformations -------------------------------------------------
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

    # -- DAG dependencies ----------------------------------------------------
    t_meltano_extract >> t_dbt_staging
    t_dbt_staging >> t_dbt_marts
    t_dbt_staging >> t_dbt_test
