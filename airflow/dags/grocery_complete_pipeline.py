"""
Grocery Complete Pipeline DAG
==============================
Runs the full grocery pipeline end-to-end:
  0. wait_for_verisim_readiness — gate on the source being ready (see
     verisim_readiness.py)
  1. grocery_ingest_api  — load all 27 source tables into raw_* schemas
  2. grocery_dbt         — transform raw → staging → marts
  3. grocery_freshness   — check source freshness (via grocery_dbt DAG)

Each child DAG runs to completion before the next starts.
Schedule: every 6 hours (00:00, 06:00, 12:00, 18:00 ET).

The readiness gate exists because provisioning unpauses this DAG as soon as the
stack is up: with catchup=False the unpause immediately starts a run, which used to
race verisim's self-bootstrap and fail on stg_pos_loyalty_point_transactions. The
run now waits for its input instead of starting and failing once.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.python import PythonSensor

from verisim_readiness import POKE_INTERVAL_S, READINESS_TIMEOUT_MIN, is_ready

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

    # --- Source readiness gate ------------------------------------------------
    # `mode="reschedule"` frees the worker slot between pokes, so waiting for a
    # fresh host's 30-day backfill costs nothing but a queued task. On a ready
    # source this costs exactly one poke (~200 ms).
    wait_for_verisim = PythonSensor(
        task_id="wait_for_verisim_readiness",
        python_callable=is_ready,
        mode="reschedule",
        poke_interval=POKE_INTERVAL_S,
        timeout=timedelta(minutes=READINESS_TIMEOUT_MIN),
        doc_md=(
            "Waits until verisim-grocery is serving data: API healthy, generator "
            "running in `realtime` mode (bootstrap + backfill finished), and the "
            "critical source tables non-empty. Prevents the first run on a fresh "
            "install from racing verisim's self-bootstrap. Tuning: "
            "`VERISIM_READINESS_TIMEOUT_MIN`, `VERISIM_READINESS_POKE_S`."
        ),
    )

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

    wait_for_verisim >> ingest >> transform
