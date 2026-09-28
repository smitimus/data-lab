"""
Grocery dbt DAG
===============
Transforms raw_* schemas → staging → marts via dbt.

Task flow:
  check_source_freshness → dbt_test_sources            (raw-layer contract gate)
    → staging (ONE dbt invocation for the whole layer — see the comment at the
      task; dbt, not Airflow, orders the models)
    → dbt_test_staging (parallel with run_intermediate)
    → run_intermediate → run_marts → dbt_test_marts

Schedule: None — trigger manually or via grocery_complete_pipeline.

Models: 32 staging views + 42 mart tables (dbt-resolved, not listed here).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

DBT = (
    "cd /opt/airflow/dbt/grocery && dbt {cmd} --profiles-dir /opt/airflow/dbt --no-use-colors"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

STAGING_SELECT = "staging"   # the layer is built by ONE dbt invocation — see the
                             # comment at staging.run_staging before splitting it.

MART_MODELS = [
    "mart_daily_revenue",
    "mart_department_performance",
    "mart_location_performance",
    "mart_product_performance",
    "mart_inventory_summary",
    "mart_supply_chain_summary",
    "mart_shrink_analysis",
    "mart_promotion_effectiveness",
    "mart_department_shrinkage",
    "mart_labor_efficiency",
    "mart_employee_productivity",
    "mart_employee_cost",
    "mart_department_labor",
    "mart_loyalty_cohort",
    "mart_attendance_summary",
    "mart_daily_attendance_stats",
    "mart_daily_fulfillment_summary",
    "mart_delivery_performance",
    "mart_employee_hours_vs_schedule",
    "mart_fleet_utilization",
    "mart_fulfillment_operations",
    "mart_fulfillment_pick_accuracy",
    "mart_hourly_sales_pattern",
    "mart_inventory_turnover",
    "mart_order_fulfillment_funnel",
    "mart_store_weekly_summary",
    "mart_transport_daily_metrics",
    "mart_transport_load_summary",
]

with DAG(
    dag_id="grocery_dbt",
    description="dbt transform grocery raw → staging → marts (32 staging, 42 mart models, dbt-resolved deps)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["grocery", "dbt"],
) as dag:

    t_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=DBT.format(cmd="source freshness"),
        execution_timeout=timedelta(minutes=5),
    )

    # ONE invocation for the whole layer (t_b48af51f), not one Airflow task per
    # model. dbt-postgres rebuilds a VIEW as
    #     alter view staging.x rename to x__dbt_backup;
    #     create view staging.x as ...;
    #     drop view staging.x__dbt_backup cascade;
    # and a view that depends on `x` FOLLOWS THE RENAME onto the backup, so that
    # trailing CASCADE deletes it. `staging.stg_pos_transaction_items` is a view
    # over `stg_pos_products` (`models/staging/stg_pos_transaction_items.sql`,
    # `{{ ref('stg_pos_products') }}`), so rebuilding `stg_pos_products` after it
    # silently drops it and nothing in the dag_run re-creates it: its 14 tests
    # and the two intermediate models reading it die on
    # `relation "staging.stg_pos_transaction_items" does not exist`, retries
    # included (CT107, 2026-09-21, ~50/50 per cycle — decided by which per-model
    # task finished second).
    #
    # One `dbt run --select staging` gives the ordering back to dbt, which builds
    # a model only after everything it refs: the swap for the dependency always
    # lands before its dependent is rebuilt, inside the same run. Per-model
    # Airflow visibility/retries in this layer are the price; in exchange a NEW
    # staging→staging `ref` cannot re-open the defect — any such pair is ordered
    # by construction. `dags/tests/test_dbt_staging_order.py` fails if this task
    # is split back into per-model tasks while a staging model refs another.
    with TaskGroup(group_id="staging") as staging_group:
        BashOperator(
            task_id="run_staging",
            bash_command=DBT.format(cmd=f"run --select {STAGING_SELECT}"),
            execution_timeout=timedelta(minutes=15),
        )

    # Raw-layer contract gate (t_77692c68). The source-level `not_null`/`unique`
    # tests declared under `sources:` in models/sources.yml hang off SOURCE nodes,
    # so no path selector (`staging`/`marts`) and no tag could ever reach them:
    # 98 of the project's 608 tests were in no DAG selection and had never run.
    # They read raw_* only, so this runs before any transform and cannot race the
    # layers it inspects — and a broken raw layer stops the run before it spends
    # 30 minutes building marts on it.
    # `source:*` also selects standalone tests whose SQL reads a source — the two
    # post_marts e2e asserts do — and those must wait for run_marts, hence the
    # exclude (same reason as dbt_test_staging's).
    t_test_sources = BashOperator(
        task_id="dbt_test_sources",
        bash_command=DBT.format(cmd="test --select source:* --exclude tag:post_marts"),
        execution_timeout=timedelta(minutes=10),
    )

    t_test_staging = BashOperator(
        task_id="dbt_test_staging",
        # post_marts tests reference mart tables (cross-layer e2e asserts) —
        # excluded here so they can't race run_marts; they ride dbt_test_marts.
        bash_command=DBT.format(cmd="test --select staging --exclude tag:post_marts"),
        execution_timeout=timedelta(minutes=10),
    )

    t_run_intermediate = BashOperator(
        task_id="run_intermediate",
        bash_command=DBT.format(cmd="run --select intermediate"),
        execution_timeout=timedelta(minutes=15),
    )

    t_run_marts = BashOperator(
        task_id="run_marts",
        bash_command=DBT.format(cmd="run --select marts"),
        execution_timeout=timedelta(minutes=30),
    )

    t_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=DBT.format(cmd="test --select marts tag:post_marts"),
        execution_timeout=timedelta(minutes=10),
    )

    t_freshness >> t_test_sources >> staging_group >> t_run_intermediate >> t_run_marts
    staging_group >> t_test_staging
    t_run_marts >> t_test_marts
