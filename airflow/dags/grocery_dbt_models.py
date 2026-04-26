"""
Grocery dbt — Per-Model DAG
============================
Granular dbt execution: one task per model, with explicit dependency wiring.

Use this DAG to:
  - Re-run a single staging model or mart without touching others
  - Debug dbt model failures in isolation
  - Run a specific mart after a targeted table ingest

Dependency structure:
  All staging models run in parallel (no inter-staging deps).
  Each mart declares explicit upstream staging deps.
  dbt_test runs in parallel with marts after staging completes.

Schedule: None — trigger manually or chain after grocery_ingest_meltano.
For automated runs use grocery_complete_pipeline (all-in-one).

Models (22 staging + 12 mart):
  Staging: stg_locations, stg_employees, stg_pos_departments, stg_pos_products,
           stg_pos_price_history, stg_pos_coupons, stg_pos_combo_deals,
           stg_pos_loyalty_members, stg_pos_loyalty_point_transactions,
           stg_pos_transactions, stg_pos_transaction_items,
           stg_timeclock_events, stg_ordering_store_orders,
           stg_ordering_store_order_items, stg_fulfillment_orders,
           stg_fulfillment_items, stg_transport_trucks, stg_transport_loads,
           stg_transport_load_items, stg_inv_products, stg_inv_stock_levels,
           stg_inv_receipts, stg_inv_receipt_items, stg_inv_shrinkage_events,
           stg_pricing_weekly_ads, stg_pricing_ad_items, stg_hr_schedules

  Marts: mart_daily_revenue, mart_department_performance, mart_location_performance,
         mart_product_performance, mart_inventory_summary, mart_supply_chain_summary,
         mart_shrink_analysis, mart_promotion_effectiveness, mart_labor_efficiency,
         mart_loyalty_cohort, mart_employee_productivity, mart_employee_cost,
         mart_department_shrinkage, mart_department_labor
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

DBT = "cd /opt/airflow/dbt/grocery && dbt run --select {model} --profiles-dir /opt/airflow/dbt --no-use-colors"
DBT_TEST = "cd /opt/airflow/dbt/grocery && dbt test --select {model} --profiles-dir /opt/airflow/dbt --no-use-colors"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------------------------------------------------------
# Staging models (all independent)
# ---------------------------------------------------------------------------

STAGING_MODELS = [
    "stg_locations",
    "stg_employees",
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
    "stg_hr_schedules",
]

# ---------------------------------------------------------------------------
# Mart definitions: (model_name, [staging_deps])
# Only staging model deps listed — mart-to-mart deps handled via dbt ref()
# ordering within this DAG uses the staging group as the upstream gate.
# ---------------------------------------------------------------------------

MART_DEFS = [
    # Revenue / sales
    ("mart_daily_revenue",          ["stg_pos_transactions", "stg_pos_transaction_items", "stg_locations"]),
    ("mart_department_performance", ["stg_pos_transaction_items", "stg_pos_transactions", "stg_pos_departments", "stg_locations"]),
    ("mart_location_performance",   ["stg_pos_transactions", "stg_locations"]),
    ("mart_product_performance",    ["stg_pos_transaction_items", "stg_pos_transactions", "stg_pos_products"]),
    # Inventory / supply chain
    ("mart_inventory_summary",      ["stg_inv_stock_levels", "stg_inv_products", "stg_pos_products"]),
    ("mart_supply_chain_summary",   ["stg_ordering_store_orders", "stg_fulfillment_orders", "stg_transport_loads", "stg_transport_trucks"]),
    # Shrinkage / promotions
    ("mart_shrink_analysis",        ["stg_inv_shrinkage_events", "stg_pos_products", "stg_locations"]),
    ("mart_promotion_effectiveness",["stg_pricing_weekly_ads", "stg_pricing_ad_items", "stg_pos_transaction_items", "stg_pos_transactions", "stg_pos_products"]),
    ("mart_department_shrinkage",   ["stg_inv_shrinkage_events", "stg_pos_transaction_items", "stg_pos_departments", "stg_locations"]),
    # Labor / HR
    ("mart_labor_efficiency",       ["stg_hr_schedules", "stg_locations"]),
    ("mart_employee_productivity",  ["stg_pos_transactions", "stg_timeclock_events", "stg_employees", "stg_locations"]),
    ("mart_employee_cost",          ["stg_hr_schedules", "stg_employees", "stg_locations", "stg_pos_transactions"]),
    ("mart_department_labor",       ["stg_hr_schedules", "stg_employees", "stg_locations"]),
    # Loyalty
    ("mart_loyalty_cohort",         ["stg_pos_loyalty_members", "stg_pos_loyalty_point_transactions", "stg_pos_transactions"]),
]


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="grocery_dbt_models",
    description="Per-model dbt run for grocery (27 staging + 14 mart models)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["grocery", "dbt", "granular"],
) as dag:

    # -- Staging group: all models run in parallel ----------------------------
    staging_tasks = {}
    with TaskGroup(group_id="staging") as staging_group:
        for model in STAGING_MODELS:
            t = BashOperator(
                task_id=model,
                bash_command=DBT.format(model=model),
                execution_timeout=timedelta(minutes=10),
            )
            staging_tasks[model] = t

    # -- dbt test on staging (runs in parallel with marts) -------------------
    t_dbt_test = BashOperator(
        task_id="dbt_test_staging",
        bash_command=DBT_TEST.format(model="staging"),
        execution_timeout=timedelta(minutes=10),
    )

    # -- Mart group: each model with explicit staging deps -------------------
    mart_tasks = {}
    with TaskGroup(group_id="marts") as marts_group:
        for model, deps in MART_DEFS:
            t = BashOperator(
                task_id=model,
                bash_command=DBT.format(model=model),
                execution_timeout=timedelta(minutes=10),
            )
            mart_tasks[model] = t

    # -- Wire: staging group gates marts + test ------------------------------
    staging_group >> marts_group
    staging_group >> t_dbt_test
