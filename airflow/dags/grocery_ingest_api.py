"""
Grocery Ingest — API Method DAG
================================
Ingests grocery source data from the Verisim HTTP API into the EDW raw schemas.
An alternative to Meltano (tap-postgres); both write to the same raw_* tables.

Strategy per table:
  - Full refresh: TRUNCATE raw table then reload all rows via paginated API calls.
  - Incremental: query MAX(watermark_col) from raw table, fetch records created
    after that timestamp. Falls back to 30 days ago on an empty table.

For a full historical backfill, pass DAG params:
  {"start_dt": "2026-01-01T00:00:00", "end_dt": "2026-03-22T23:59:59"}
Incremental tables will use those bounds instead of the watermark.

Tables: 27 across 8 schemas (same coverage as grocery_ingest_tables / Meltano).

Schedule: None — trigger manually or via Airflow API.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection config
# ---------------------------------------------------------------------------

import os
API_BASE = f"http://{os.getenv('IP', '127.0.0.1')}:8010"
EDW_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "grocery",
    "user": "postgres",
    "password": "postgres",
}
PAGE_SIZE = 2000
INCREMENTAL_FALLBACK_DAYS = 30   # lookback when raw table is empty

# ---------------------------------------------------------------------------
# Table registry
# Each entry: (task_id, api_path, raw_schema, raw_table, pk_col,
#              strategy, watermark_col, api_start_param, api_end_param)
#
# strategy "full"        — TRUNCATE + reload all rows (no time filter)
# strategy "incremental" — fetch rows since MAX(watermark_col) in raw table
# ---------------------------------------------------------------------------

TABLE_CONFIGS = [
    # ── HR ──────────────────────────────────────────────────────────────────
    ("hr_locations",    "/grocery/hr/locations",
     "raw_hr",  "locations",  "location_id",
     "full", None, None, None),

    ("hr_employees",    "/grocery/hr/employees",
     "raw_hr",  "employees",  "employee_id",
     "full", None, None, None),

    ("hr_schedules",    "/grocery/hr/schedules",
     "raw_hr",  "schedules",  "schedule_id",
     "incremental", "created_at", "start_date", "end_date"),

    # ── POS ─────────────────────────────────────────────────────────────────
    ("pos_departments", "/grocery/pos/departments",
     "raw_pos", "departments", "department_id",
     "full", None, None, None),

    ("pos_products",    "/grocery/pos/products",
     "raw_pos", "products",   "product_id",
     "full", None, None, None),

    ("pos_price_history", "/grocery/pos/price-history",
     "raw_pos", "price_history", "price_history_id",
     "full", None, None, None),

    ("pos_coupons",     "/grocery/pos/coupons",
     "raw_pos", "coupons",    "coupon_id",
     "full", None, None, None),

    ("pos_combo_deals", "/grocery/pos/combo-deals",
     "raw_pos", "combo_deals", "deal_id",
     "full", None, None, None),

    ("pos_loyalty_members", "/grocery/pos/loyalty-members",
     "raw_pos", "loyalty_members", "member_id",
     "full", None, None, None),

    ("pos_loyalty_point_transactions", "/grocery/pos/loyalty-point-transactions",
     "raw_pos", "loyalty_point_transactions", "pt_id",
     "incremental", "created_at", "start_dt", "end_dt"),

    ("pos_transactions", "/grocery/pos/transactions",
     "raw_pos", "transactions", "transaction_id",
     "incremental", "transaction_dt", "start_dt", "end_dt"),

    ("pos_transaction_items", "/grocery/pos/transaction-items",
     "raw_pos", "transaction_items", "item_id",
     "incremental", "transaction_dt", "start_dt", "end_dt"),

    # ── Timeclock ────────────────────────────────────────────────────────────
    ("timeclock_events", "/grocery/timeclock/events",
     "raw_timeclock", "events", "event_id",
     "incremental", "event_dt", "start_dt", "end_dt"),

    # ── Ordering ─────────────────────────────────────────────────────────────
    ("ordering_store_orders", "/grocery/ordering/orders",
     "raw_ordering", "store_orders", "order_id",
     "full", None, None, None),

    ("ordering_store_order_items", "/grocery/ordering/order-items",
     "raw_ordering", "store_order_items", "item_id",
     "full", None, None, None),

    # ── Fulfillment ──────────────────────────────────────────────────────────
    ("fulfillment_orders", "/grocery/fulfillment/orders",
     "raw_fulfillment", "orders", "fulfillment_id",
     "full", None, None, None),

    ("fulfillment_items", "/grocery/fulfillment/items",
     "raw_fulfillment", "items", "item_id",
     "full", None, None, None),

    # ── Transport ────────────────────────────────────────────────────────────
    ("transport_trucks", "/grocery/transport/trucks",
     "raw_transport", "trucks", "truck_id",
     "full", None, None, None),

    ("transport_loads", "/grocery/transport/loads",
     "raw_transport", "loads", "load_id",
     "full", None, None, None),

    ("transport_load_items", "/grocery/transport/load-items",
     "raw_transport", "load_items", "item_id",
     "full", None, None, None),

    # ── Inventory ────────────────────────────────────────────────────────────
    ("inv_products", "/grocery/inventory/products",
     "raw_inv", "products", "inv_product_id",
     "full", None, None, None),

    ("inv_stock_levels", "/grocery/inventory/stock-levels",
     "raw_inv", "stock_levels", "stock_id",
     "full", None, None, None),

    ("inv_receipts", "/grocery/inventory/receipts",
     "raw_inv", "receipts", "receipt_id",
     "incremental", "received_dt", "start_dt", "end_dt"),

    ("inv_receipt_items", "/grocery/inventory/receipt-items",
     "raw_inv", "receipt_items", "receipt_item_id",
     "incremental", "received_dt", "start_dt", "end_dt"),

    ("inv_shrinkage_events", "/grocery/inventory/shrinkage-events",
     "raw_inv", "shrinkage_events", "shrinkage_id",
     "incremental", "recorded_at", "start_dt", "end_dt"),

    # ── Pricing ──────────────────────────────────────────────────────────────
    ("pricing_weekly_ads", "/grocery/pricing/weekly-ads",
     "raw_pricing", "weekly_ads", "ad_id",
     "full", None, None, None),

    ("pricing_ad_items", "/grocery/pricing/ad-items",
     "raw_pricing", "ad_items", "ad_item_id",
     "full", None, None, None),
]

# Schema prefix → TaskGroup label
_PREFIX_TO_GROUP = {
    "hr_": "hr",
    "pos_": "pos",
    "timeclock_": "timeclock",
    "ordering_": "ordering",
    "fulfillment_": "fulfillment",
    "transport_": "transport",
    "inv_": "inv",
    "pricing_": "pricing",
}


def _schema_group(task_id: str) -> str:
    for prefix, group in _PREFIX_TO_GROUP.items():
        if task_id.startswith(prefix):
            return group
    return "other"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _edw_conn():
    return psycopg2.connect(**EDW_CONN)


def _raw_columns(conn, schema: str, table: str) -> list:
    """Return non-_sdc column names for the raw table in ordinal order."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND column_name NOT LIKE '_sdc%%'
            ORDER BY ordinal_position
        """, [schema, table])
        return [r[0] for r in cur.fetchall()]


def _get_watermark(conn, schema: str, table: str, col: str) -> str:
    """MAX(col) from raw table; falls back to INCREMENTAL_FALLBACK_DAYS ago."""
    with conn.cursor() as cur:
        cur.execute(f'SELECT MAX("{col}") FROM "{schema}"."{table}"')
        result = cur.fetchone()[0]
    if result is None:
        fb = datetime.now(timezone.utc) - timedelta(days=INCREMENTAL_FALLBACK_DAYS)
        return fb.isoformat()
    return result.isoformat() if isinstance(result, datetime) else str(result)


def _fetch_all(path: str, params: dict) -> list:
    """Paginate through API endpoint, return all rows."""
    url = f"{API_BASE}{path}"
    rows = []
    offset = 0
    while True:
        p = {**params, "limit": PAGE_SIZE, "offset": offset}
        resp = requests.get(url, params=p, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        page = data if isinstance(data, list) else data.get("data", [])
        if not page:
            break
        rows.extend(page)
        log.info("  %s: fetched %d rows (offset=%d)", path, len(page), offset)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def _coerce(val: Any) -> Any:
    if isinstance(val, (dict, list)):
        import json
        return json.dumps(val)
    return val


def _upsert_rows(conn, schema: str, table: str, rows: list,
                 pk_col: str, raw_cols: list, extracted_at: str) -> int:
    """Upsert rows into raw table; sets _sdc metadata columns. Returns count."""
    if not rows:
        return 0

    # Only insert columns present in both the raw table schema and the API response
    insert_cols = [c for c in raw_cols if c in rows[0]]
    all_cols = insert_cols + ["_sdc_extracted_at", "_sdc_batched_at", "_sdc_deleted_at"]

    col_sql = ", ".join(f'"{c}"' for c in all_cols)
    val_sql = ", ".join(["%s"] * len(all_cols))
    upd_sql = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in all_cols
        if c != pk_col
    )
    sql = (
        f'INSERT INTO "{schema}"."{table}" ({col_sql}) VALUES ({val_sql}) '
        f'ON CONFLICT ("{pk_col}") DO UPDATE SET {upd_sql}'
    )

    records = [
        [_coerce(row.get(c)) for c in insert_cols] + [extracted_at, extracted_at, None]
        for row in rows
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=500)
    conn.commit()
    return len(records)


# ---------------------------------------------------------------------------
# Core callable — one PythonOperator per table calls this
# ---------------------------------------------------------------------------

def ingest_table(
    task_id: str,
    api_path: str,
    raw_schema: str,
    raw_table: str,
    pk_col: str,
    strategy: str,
    watermark_col,
    api_start_param,
    api_end_param,
    **context,
) -> None:
    params_conf = context.get("params") or {}
    now_iso = datetime.now(timezone.utc).isoformat()

    conn = _edw_conn()
    try:
        raw_cols = _raw_columns(conn, raw_schema, raw_table)
        log.info("[%s] raw table columns: %s", task_id, raw_cols)

        if strategy == "full":
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE "{raw_schema}"."{raw_table}"')
            conn.commit()
            log.info("[%s] truncated %s.%s", task_id, raw_schema, raw_table)
            rows = _fetch_all(api_path, {})

        else:  # incremental
            if params_conf.get("start_dt") and params_conf.get("end_dt"):
                start = params_conf["start_dt"]
                end = params_conf["end_dt"]
                log.info("[%s] param window: %s → %s", task_id, start, end)
            else:
                start = _get_watermark(conn, raw_schema, raw_table, watermark_col)
                end = now_iso
                log.info("[%s] watermark window: %s → %s", task_id, start, end)

            rows = _fetch_all(api_path, {api_start_param: start, api_end_param: end})

        log.info("[%s] fetched %d total rows", task_id, len(rows))
        written = _upsert_rows(conn, raw_schema, raw_table, rows, pk_col, raw_cols, now_iso)
        log.info("[%s] wrote %d rows to %s.%s", task_id, written, raw_schema, raw_table)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG — group configs by schema, build one TaskGroup per schema
# ---------------------------------------------------------------------------

# Pre-group the table configs by schema label
grouped: dict = defaultdict(list)
for cfg in TABLE_CONFIGS:
    grouped[_schema_group(cfg[0])].append(cfg)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="grocery_ingest_api",
    description=(
        "API-based ingestion grocery source → EDW raw (27 tables, 8 schemas). "
        "Alternative to Meltano tap-postgres. Pass {start_dt, end_dt} params "
        "for incremental backfill override."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"start_dt": None, "end_dt": None},
    tags=["grocery", "api", "ingest", "granular"],
) as dag:

    for schema, table_list in grouped.items():
        with TaskGroup(group_id=f"ingest_{schema}"):
            for (tid, api_path, raw_schema, raw_table, pk_col,
                 strategy, watermark_col, api_start_param, api_end_param) in table_list:
                PythonOperator(
                    task_id=tid,
                    python_callable=ingest_table,
                    op_kwargs={
                        "task_id": tid,
                        "api_path": api_path,
                        "raw_schema": raw_schema,
                        "raw_table": raw_table,
                        "pk_col": pk_col,
                        "strategy": strategy,
                        "watermark_col": watermark_col,
                        "api_start_param": api_start_param,
                        "api_end_param": api_end_param,
                    },
                    execution_timeout=timedelta(minutes=20),
                )
