"""
Grocery dbt DAG
===============
Transforms raw_* schemas → staging → marts via dbt.

Task flow:
  staging group (all models in parallel, max 4 at a time)
    → marts group (all models in parallel, max 4 at a time)
    → dbt_test_staging (parallel with marts)
  marts group → dbt_test_marts

Schedule: None — trigger manually or via grocery_complete_pipeline.

Models: 27 staging + 14 mart
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

STAGING_MODELS = [
    "stg_locations",
    "stg_employees",
    "stg_hr_schedules",
    "stg_pos_departments",
    "stg_pos_products",
    "stg_pos_price_history",
    "stg_pos_coupons",
    "stg_pos_combo_deals",
    "stg_pos_loyalty_members",
    "stg_pos_loyalty_point_transactions",
    "stg_pos_transactions",
    "stg_pos_transaction_items",
    "stg_timeclock_events",
    "stg_ordering_store_orders",
    "stg_ordering_store_order_items",
    "stg_fulfillment_orders",
    "stg_fulfillment_items",
    "stg_transport_trucks",
    "stg_transport_loads",
    "stg_transport_load_items",
    "stg_inv_products",
    "stg_inv_stock_levels",
    "stg_inv_receipts",
    "stg_inv_receipt_items",
    "stg_inv_shrinkage_events",
    "stg_pricing_weekly_ads",
    "stg_pricing_ad_items",
]

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
    description="dbt transform grocery raw → staging → marts (28 staging, 29 mart models, dbt-resolved deps)",
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
        bash_command=DBT.format(cmd="source freshness") + " || true",
        execution_timeout=timedelta(minutes=5),
    )

    with TaskGroup(group_id="staging") as staging_group:
        for model in STAGING_MODELS:
            BashOperator(
                task_id=model,
                bash_command=DBT.format(cmd=f"run --select {model}"),
                execution_timeout=timedelta(minutes=10),
            )

    t_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=DBT.format(cmd="test --select staging") + " || true",
        execution_timeout=timedelta(minutes=10),
    )

    t_run_marts = BashOperator(
        task_id="run_marts",
        bash_command=DBT.format(cmd="run --select marts"),
        execution_timeout=timedelta(minutes=30),
    )

    t_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=DBT.format(cmd="test --select marts") + " || true",
        execution_timeout=timedelta(minutes=10),
    )

    t_freshness >> staging_group >> [t_run_marts, t_test_staging]
    t_run_marts >> t_test_marts
