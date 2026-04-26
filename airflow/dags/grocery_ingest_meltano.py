"""
Grocery Ingest — Per-Table DAG
==============================
Granular Meltano ingestion: one task per source table, grouped by schema.
Tasks within each schema group run in parallel; schema groups run in parallel.

Use this DAG to:
  - Re-ingest a single table without running the full pipeline
  - Debug ingestion issues at the table level
  - Run targeted backfills for specific tables

Meltano command uses `meltano el` with `--select` to target a single stream.
Stream naming: pipelinewise-tap-postgres uses '<schema>-<table>' format.

Schedule: None — trigger manually or via Airflow API.
For automated runs use grocery_complete_pipeline (all-in-one).

Tables covered (27 streams across 8 schemas):
  hr (3): locations, employees, schedules
  pos (9): departments, products, price_history, coupons, combo_deals,
           loyalty_members, loyalty_point_transactions, transactions, transaction_items
  timeclock (1): events
  ordering (2): store_orders, store_order_items
  fulfillment (2): orders, items
  transport (3): trucks, loads, load_items
  inv (5): products, stock_levels, receipts, receipt_items, shrinkage_events
  pricing (2): weekly_ads, ad_items
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# pipelinewise-tap-postgres FULL_TABLE replication generates a timestamp-based
# surrogate PK column (e.g. product_id_20260426_1428) that changes each run.
# Dropping raw schemas before each run lets pipelinewise recreate tables fresh,
# avoiding NOT NULL violations from the previous run's surrogate key columns.
CLEAR_RAW_SCHEMAS = """
docker exec postgres psql -U postgres -d grocery -c "
  DROP SCHEMA IF EXISTS raw_hr CASCADE;
  DROP SCHEMA IF EXISTS raw_pos CASCADE;
  DROP SCHEMA IF EXISTS raw_timeclock CASCADE;
  DROP SCHEMA IF EXISTS raw_ordering CASCADE;
  DROP SCHEMA IF EXISTS raw_fulfillment CASCADE;
  DROP SCHEMA IF EXISTS raw_transport CASCADE;
  DROP SCHEMA IF EXISTS raw_inv CASCADE;
  DROP SCHEMA IF EXISTS raw_pricing CASCADE;
"
"""

MELTANO_RUN = (
    "docker exec postgres psql -U postgres -d grocery "
    "-c 'DROP TABLE IF EXISTS \"raw_{schema}\".\"{table}\" CASCADE' ; "
    "docker exec meltano meltano --cwd /project el "
    "tap-postgres-grocery target-postgres-grocery "
    "--select '{stream}' "
    "--state-id grocery-{stream} "
    "--force"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------------------------------------------------------
# Table definitions: (task_id, stream_name)
# stream format: <schema>-<table> (pipelinewise convention)
# ---------------------------------------------------------------------------

SCHEMA_TABLES = {
    "hr": [
        ("locations",                   "hr-locations"),
        ("employees",                   "hr-employees"),
        ("schedules",                   "hr-schedules"),
    ],
    "pos": [
        ("departments",                 "pos-departments"),
        ("products",                    "pos-products"),
        ("price_history",               "pos-price_history"),
        ("coupons",                     "pos-coupons"),
        ("combo_deals",                 "pos-combo_deals"),
        ("loyalty_members",             "pos-loyalty_members"),
        ("loyalty_point_transactions",  "pos-loyalty_point_transactions"),
        ("transactions",                "pos-transactions"),
        ("transaction_items",           "pos-transaction_items"),
    ],
    "timeclock": [
        ("events",                      "timeclock-events"),
    ],
    "ordering": [
        ("store_orders",                "ordering-store_orders"),
        ("store_order_items",           "ordering-store_order_items"),
    ],
    "fulfillment": [
        ("orders",                      "fulfillment-orders"),
        ("items",                       "fulfillment-items"),
    ],
    "transport": [
        ("trucks",                      "transport-trucks"),
        ("loads",                       "transport-loads"),
        ("load_items",                  "transport-load_items"),
    ],
    "inv": [
        ("products",                    "inv-products"),
        ("stock_levels",                "inv-stock_levels"),
        ("receipts",                    "inv-receipts"),
        ("receipt_items",               "inv-receipt_items"),
        ("shrinkage_events",            "inv-shrinkage_events"),
    ],
    "pricing": [
        ("weekly_ads",                  "pricing-weekly_ads"),
        ("ad_items",                    "pricing-ad_items"),
    ],
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="grocery_ingest_meltano",
    description="Per-table Meltano ingestion for grocery source (27 tables, 8 schemas)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["grocery", "meltano", "ingest", "granular"],
) as dag:

    clear_schemas = BashOperator(
        task_id="clear_raw_schemas",
        bash_command=CLEAR_RAW_SCHEMAS,
    )

    for schema, tables in SCHEMA_TABLES.items():
        with TaskGroup(group_id=f"ingest_{schema}") as tg:
            for task_name, stream in tables:
                BashOperator(
                    task_id=task_name,
                    bash_command=MELTANO_RUN.format(
                        schema=schema, table=task_name, stream=stream,
                    ),
                    execution_timeout=timedelta(minutes=20),
                )
        clear_schemas >> tg  # all schema groups wait for raw schemas to be cleared
