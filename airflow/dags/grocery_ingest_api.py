"""
Grocery Ingest — API Method DAG
================================
Ingests grocery source data from the Verisim HTTP API into the EDW raw schemas.
This is the sole ingestion path into the EDW (Meltano has been removed from the stack).

Strategy per table:
  - Full refresh: TRUNCATE raw table then reload all rows via paginated API calls.
  - Incremental: query MAX(watermark_col) from raw table, fetch records created
    after that timestamp. Falls back to 365 days ago on an empty table.

The source (Verisim HTTP API + source DB) is addressed by Docker service name on
the shared network — see the `datalab_shared` network in
verisim-grocery/compose.yaml. It is deliberately not derived from the host's
`IP` env var: a stale IP makes the ingest read another instance's dataset
instead of failing (t_05b48b69).

The DAG ends with `verify_raw_vs_source`, an invariant that compares every raw
table's row count against its source relation and fails the run when the EDW
holds more rows than the source. See SOURCE_RELATIONS for the mapping.

For a full historical backfill, pass DAG params:
  {"start_dt": "2026-01-01T00:00:00", "end_dt": "2026-03-22T23:59:59"}
Incremental tables will use those bounds instead of the watermark.

Tables: 32 across 9 schemas.

Schedule: None — trigger manually or via Airflow API.
"""
from __future__ import annotations

import logging
import time
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

# ---------------------------------------------------------------------------
# Source address — the Verisim instance that belongs to THIS stack
# ---------------------------------------------------------------------------
# Resolved by Docker service name over the shared network (see the
# `datalab_shared` network in verisim-grocery/compose.yaml). Both the HTTP API
# and the source DB are addressed this way, and deliberately NOT through the
# host's `IP` env var.
#
# Why: `IP` is a host-specific value baked into the container environment when
# the container is created. When it goes stale the ingest does not fail — it
# quietly reads a *different* instance's dataset and writes it into our EDW as
# if it were ours (t_05b48b69, 2026-09-21: the worker still carried
# IP=192.168.1.7 from the host being replaced, so a fresh dev instance ingested
# 1,136,360 transactions from the old instance while its own source held
# 98,112 — and then spent hours pulling that instance's 6.6M transaction_items).
# Service DNS cannot drift: it resolves inside the stack or the task fails.
VERISIM_API_URL = os.getenv("VERISIM_API_URL", "http://verisim-grocery:8000")
VERISIM_DB = {
    "host": os.getenv("VERISIM_DB_HOST", "verisim-grocery"),
    "port": int(os.getenv("VERISIM_DB_PORT", "5432")),
    "dbname": os.getenv("VERISIM_DB_NAME", "grocery"),
    "user": os.getenv("VERISIM_DB_USER", "verisim"),
    "password": os.getenv("VERISIM_DB_PASSWORD", "verisim"),
}

EDW_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "grocery",
    "user": "postgres",
    "password": "postgres",
}
PAGE_SIZE = 1000
API_MAX_LIMIT = 1000  # Verisim API cap (returns 422 if exceeded)
SMALL_TABLE_THRESHOLD = API_MAX_LIMIT  # full-refresh tables under this limit fetch in one request (avoids offset-pagination race)
MAX_PAGES = 10_000  # safety cap: fail if pagination exceeds this (infinite loop guard for volatile endpoints)
INCREMENTAL_FALLBACK_DAYS = 365  # lookback when raw table is empty
FETCH_RETRIES = 4  # attempts per page request before failing the task (no partial-load-on-error)
FETCH_RETRY_BACKOFF = 2.0  # seconds; exponential per attempt, capped at 30s

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
     "full", None, None, None),

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

    ("pos_returns", "/grocery/pos/returns",
     "raw_pos", "returns", "return_id",
     "incremental", "return_dt", "start_dt", "end_dt"),

    ("pos_return_items", "/grocery/pos/return-items",
     "raw_pos", "return_items", "return_item_id",
     "full", None, None, None),

    # ── Online (e-commerce orders, t_24fae529) ──────────────────────────────
    ("online_orders", "/grocery/online/orders",
     "raw_online", "orders", "order_id",
     "incremental", "placed_dt", "start_dt", "end_dt"),

    ("online_order_items", "/grocery/online/order-items",
     "raw_online", "order_items", "item_id",
     "incremental", "placed_dt", "start_dt", "end_dt"),

    ("online_order_events", "/grocery/online/order-events",
     "raw_online", "order_events", "event_id",
     "full", None, None, None),

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

# ---------------------------------------------------------------------------
# Reconciliation contract: raw table → source relation
# ---------------------------------------------------------------------------
# The raw table each task fills and the Verisim relation that task is supposed
# to be a copy of. This is what makes the post-ingest invariant
# (verify_raw_vs_source) possible: without an explicit mapping there is no way
# to tell "loaded fewer rows because the endpoint filters" from "loaded a
# different instance's data".
#
# When adding a table to TABLE_CONFIGS, add it here too — the module asserts
# the two stay in sync, so a miss fails at DAG parse time rather than silently
# leaving a table unreconciled.
#
# The API path is NOT a reliable derivation of this: the endpoints spell tables
# with hyphens and the mapping is not 1:1 by name
# (ordering/order-items → ordering.store_order_items).
SOURCE_RELATIONS = {
    # task_id                         source relation
    "hr_locations":                   "hr.locations",
    "hr_employees":                   "hr.employees",
    "hr_schedules":                   "hr.schedules",
    "pos_departments":                "pos.departments",
    "pos_products":                   "pos.products",
    "pos_price_history":              "pos.price_history",
    "pos_coupons":                    "pos.coupons",
    "pos_combo_deals":                "pos.combo_deals",
    "pos_loyalty_members":            "pos.loyalty_members",
    "pos_loyalty_point_transactions": "pos.loyalty_point_transactions",
    "pos_transactions":               "pos.transactions",
    "pos_transaction_items":          "pos.transaction_items",
    "pos_returns":                    "pos.returns",
    "pos_return_items":               "pos.return_items",
    "online_orders":                  "online.orders",
    "online_order_items":             "online.order_items",
    "online_order_events":            "online.order_events",
    "timeclock_events":               "timeclock.events",
    "ordering_store_orders":          "ordering.store_orders",
    "ordering_store_order_items":     "ordering.store_order_items",
    "fulfillment_orders":             "fulfillment.orders",
    "fulfillment_items":              "fulfillment.items",
    "transport_trucks":               "transport.trucks",
    "transport_loads":                "transport.loads",
    "transport_load_items":           "transport.load_items",
    "inv_products":                   "inv.products",
    "inv_stock_levels":               "inv.stock_levels",
    "inv_receipts":                   "inv.receipts",
    "inv_receipt_items":              "inv.receipt_items",
    "inv_shrinkage_events":           "inv.shrinkage_events",
    "pricing_weekly_ads":             "pricing.weekly_ads",
    "pricing_ad_items":               "pricing.ad_items",
}

# Endpoints that intentionally serve a SUBSET of their source relation
# (`active_only=True` by default in the Verisim API). For these a shortfall is
# expected and is only reported; an *excess* is always a failure.
SOURCE_PARTIAL = {"pos_coupons", "pos_combo_deals"}

_unmapped = sorted(c[0] for c in TABLE_CONFIGS if c[0] not in SOURCE_RELATIONS)
if _unmapped:
    raise ValueError(
        "SOURCE_RELATIONS is missing entries for configured table(s): "
        + ", ".join(_unmapped)
    )


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
    "online_": "online",
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


def _request_page(url: str, p: dict, timeout_s: int, path: str) -> requests.Response:
    """GET one page, retrying transport errors and 5xx. Raises RuntimeError when
    retries are exhausted — a partial load must never look like success."""
    last_err = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            resp = requests.get(url, params=p, timeout=timeout_s)
        except requests.RequestException as exc:
            last_err = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code < 500:
                return resp
            last_err = f"HTTP {resp.status_code}"
        if attempt < FETCH_RETRIES:
            sleep_s = min(FETCH_RETRY_BACKOFF * (2 ** (attempt - 1)), 30)
            log.warning(
                "  %s: %s on offset=%s (attempt %d/%d) — retrying in %.0fs",
                path, last_err, p.get("offset"), attempt, FETCH_RETRIES, sleep_s,
            )
            time.sleep(sleep_s)
    raise RuntimeError(
        f"{path}: API request failed after {FETCH_RETRIES} attempts "
        f"(offset={p.get('offset')}) — last error: {last_err}"
    )


def _detect_schema_drift(conn, schema: str, table: str,
                         api_cols: list, raw_cols: list, task_id: str) -> list:
    """Harden the verisim<->dbt cross-repo contract against schema drift.

    Columns present in the API payload but missing in the raw table are added
    as TEXT (ALTER) with a WARNING — the dbt staging model must be updated in
    lockstep. Columns in raw but missing from the API response are logged as
    drift (data for them will land as NULL). Returns the refreshed column list.
    """
    api_cols = [c for c in api_cols if not c.startswith("_sdc")]
    new_cols = [c for c in api_cols if c not in raw_cols]
    missing_on_api_side = [c for c in raw_cols if c not in api_cols]
    if new_cols:
        with conn.cursor() as cur:
            for c in new_cols:
                cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{c}" TEXT')
        conn.commit()
        log.warning(
            "[%s] SCHEMA DRIFT: %s.%s — %d new API column(s) added via ALTER: %s. "
            "Update the dbt staging model in lockstep.",
            task_id, schema, table, len(new_cols), ", ".join(new_cols),
        )
        raw_cols = _raw_columns(conn, schema, table)
    if missing_on_api_side:
        log.warning(
            "[%s] SCHEMA DRIFT: %s.%s — %d raw column(s) absent from API payload "
            "(rename/removal?): %s",
            task_id, schema, table, len(missing_on_api_side),
            ", ".join(missing_on_api_side),
        )
    return raw_cols


def _assert_complete(path: str, consumed: int, snapshot_total, seen: int) -> None:
    """Fail loudly when pagination ended before the API's advertised row count."""
    if snapshot_total is not None and consumed < snapshot_total:
        raise RuntimeError(
            f"{path}: pagination stopped after {consumed} rows but API reported "
            f"total={snapshot_total} (offset={seen}) — refusing partial load"
        )


def _fetch_pages(path: str, params: dict, max_page_fetch: int | None = None):
    """Generator: yield one page of rows at a time, never accumulating all rows in memory.

    Pagination stops when one of these conditions is met (checked in order):
    1. Empty page → source has no more data (return)
    2. Page has fewer rows than requested → last page (return)
    3. Page count exceeds MAX_PAGES → RuntimeError (infinite-loop guard)
    4. offset >= snapshot_total from the API's `total` field → boundary reached (return)

    When max_page_fetch is set the first request uses that limit (avoids offset-pagination
    race for small tables), then falls back to PAGE_SIZE for subsequent pages.

    Transport errors and 5xx responses are retried (see _request_page) and raise
    RuntimeError once retries are exhausted. If the API advertised a `total` and
    pagination ends with fewer rows than that snapshot, RuntimeError is raised —
    silently-dropped rows are a partial load posing as success.
    """
    url = f"{VERISIM_API_URL}{path}"
    page_limit = max_page_fetch or PAGE_SIZE
    offset = 0
    snapshot_total = None
    pages = 0
    fetched = 0
    timeout_s = 120 if max_page_fetch else 60

    while True:
        p = {**params, "limit": page_limit, "offset": offset}
        resp = _request_page(url, p, timeout_s, path)
        resp.raise_for_status()
        data = resp.json()
        page = data if isinstance(data, list) else data.get("data", [])

        # Capture the snapshot total from the first API response if available
        if snapshot_total is None and isinstance(data, dict) and "total" in data:
            snapshot_total = data["total"]
            log.info("  %s: API reports total=%d rows", path, snapshot_total)

        if not page:
            _assert_complete(path, fetched, snapshot_total, offset)
            return

        pages += 1
        if pages > MAX_PAGES:
            raise RuntimeError(
                f"{path}: pagination exceeded {MAX_PAGES} pages — "
                f"infinite loop detected on volatile endpoint (offset={offset})"
            )

        fetched += len(page)
        log.info("  %s: fetched %d rows (offset=%d, page=%d)", path, len(page), offset, pages)
        yield page

        # Last page: returned fewer rows than requested
        if len(page) < page_limit:
            _assert_complete(path, fetched, snapshot_total, offset + len(page))
            return

        offset += page_limit

        # Snapshot boundary: we've consumed all rows that existed when we started
        if snapshot_total is not None and offset >= snapshot_total:
            log.info("  %s: reached snapshot boundary (offset=%d, total=%d)", path, offset, snapshot_total)
            return

        # After the first oversized request, switch to standard page size
        if page_limit != PAGE_SIZE:
            page_limit = PAGE_SIZE
            timeout_s = 60


def _coerce(val: Any) -> Any:
    if isinstance(val, (dict, list)):
        import json
        return json.dumps(val)
    return val


def _ensure_table(conn, schema: str, table: str, pk_col: str, sample_row: dict) -> None:
    """Create schema and table from a sample API row if they don't exist."""
    cols = [c for c in sample_row.keys() if not c.startswith("_sdc")]
    col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                {col_defs},
                "_sdc_extracted_at" TEXT,
                "_sdc_batched_at"   TEXT,
                "_sdc_deleted_at"   TEXT,
                PRIMARY KEY ("{pk_col}")
            )
        """)
    conn.commit()


def _upsert_rows(conn, schema: str, table: str, rows: list,
                 pk_col: str, raw_cols: list, extracted_at: str) -> int:
    """Upsert rows into raw table; sets _sdc metadata columns. Returns count."""
    if not rows:
        return 0

    # Only insert columns present in both the raw table schema and the API response.
    # (After _detect_schema_drift ran on the first page, a drop here means the API
    # grew a column mid-pagination — log it loudly instead of swallowing it.)
    api_keys = set()
    for row in rows[:50]:
        api_keys.update(k for k in row.keys() if not k.startswith("_sdc"))
    insert_cols = [c for c in raw_cols if c in api_keys]
    dropped_cols = sorted(api_keys - set(raw_cols))
    if dropped_cols:
        log.warning(
            "  %s.%s: %d API column(s) not in raw table (dropped this page): %s",
            schema, table, len(dropped_cols), ", ".join(dropped_cols),
        )
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
        # Ensure schema exists before any table operation
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{raw_schema}"')
        conn.commit()

        raw_cols = _raw_columns(conn, raw_schema, raw_table)
        table_exists = bool(raw_cols)
        log.info("[%s] raw table columns: %s", task_id, raw_cols)

        # If the table exists but pk_col is absent (e.g. created by a different tool
        # with a different schema), drop it so _ensure_table recreates it correctly.
        if table_exists and pk_col not in raw_cols:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE "{raw_schema}"."{raw_table}" CASCADE')
            conn.commit()
            log.info("[%s] dropped %s.%s (pk_col missing — wrong schema)", task_id, raw_schema, raw_table)
            raw_cols = []
            table_exists = False

        if strategy == "full":
            if table_exists:
                with conn.cursor() as cur:
                    cur.execute(f'TRUNCATE "{raw_schema}"."{raw_table}"')
                conn.commit()
                log.info("[%s] truncated %s.%s for full reload", task_id, raw_schema, raw_table)
            # If the endpoint requires date params, pass a wide window covering all history
            if api_start_param:
                fetch_params: dict = {api_start_param: "2000-01-01", api_end_param: now_iso[:10]}
                log.info("[%s] full reload with date window 2000-01-01 → %s", task_id, now_iso[:10])
            else:
                fetch_params = {}

        else:  # incremental
            if params_conf.get("start_dt") and params_conf.get("end_dt"):
                start = params_conf["start_dt"]
                end = params_conf["end_dt"]
                log.info("[%s] param window: %s → %s", task_id, start, end)
            elif table_exists:
                start = _get_watermark(conn, raw_schema, raw_table, watermark_col)
                end = now_iso
                log.info("[%s] watermark window: %s → %s", task_id, start, end)
            else:
                fb = datetime.now(timezone.utc) - timedelta(days=INCREMENTAL_FALLBACK_DAYS)
                start = fb.isoformat()
                end = now_iso
                log.info("[%s] no table yet — fallback window: %s → %s", task_id, start, end)

            # Some endpoints expect date-only (YYYY-MM-DD) not full ISO timestamps
            def _fmt(val, param):
                return val[:10] if param and param.endswith("_date") else val
            fetch_params = {
                api_start_param: _fmt(start, api_start_param),
                api_end_param:   _fmt(end,   api_end_param),
            }

        # Determine fetch mode: small full-refresh tables use a single request
        # to avoid offset-pagination race (generator adding rows mid-ingestion).
        max_fetch = SMALL_TABLE_THRESHOLD if strategy == "full" else None

        # Stream page-by-page into Postgres — never hold the full dataset in memory
        written = 0
        page_num = 0
        for page in _fetch_pages(api_path, fetch_params, max_page_fetch=max_fetch):
            if not table_exists:
                _ensure_table(conn, raw_schema, raw_table, pk_col, page[0])
                raw_cols = _raw_columns(conn, raw_schema, raw_table)
                table_exists = True
                log.info("[%s] created table %s.%s", task_id, raw_schema, raw_table)
            else:
                # Schema-drift gate: API payload columns must be a superset check
                # against raw columns — new ones ALTER+warn, vanished ones warn.
                api_cols = list(page[0].keys())
                raw_cols = _detect_schema_drift(
                    conn, raw_schema, raw_table, api_cols, raw_cols, task_id
                )
            n = _upsert_rows(conn, raw_schema, raw_table, page, pk_col, raw_cols, now_iso)
            written += n
            page_num += 1
            log.info("[%s] page %d: inserted %d rows (total so far: %d)", task_id, page_num, n, written)

        log.info("[%s] done — %d total rows written to %s.%s", task_id, written, raw_schema, raw_table)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Post-ingest invariant — raw vs source row counts
# ---------------------------------------------------------------------------

def _source_counts(relations: list) -> tuple:
    """count(*) per source relation, read from the Verisim source DB.

    Returns (counts, errors): a relation that cannot be counted maps to None in
    counts and carries its reason in errors. A source image older than this
    DAG's table registry — or a wrong mapping — must surface as a named contract
    violation, not as a psycopg2 traceback in the middle of the invariant.
    """
    conn = psycopg2.connect(**VERISIM_DB)
    counts, errors = {}, {}
    try:
        for rel in relations:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {rel}")
                    counts[rel] = cur.fetchone()[0]
            except psycopg2.Error as exc:
                # The failed statement aborts the transaction; clear it or every
                # following COUNT dies with InFailedSqlTransaction.
                conn.rollback()
                reason = str(exc).strip().splitlines()[0]
                counts[rel] = None
                errors[rel] = reason
                log.warning("[verify] source relation %s is unreadable: %s", rel, reason)
    finally:
        conn.close()
    return counts, errors


def _assert_api_is_this_source() -> None:
    """Cheap identity probe: the API and the source DB must be the same instance.

    Both are resolved by service name, so this only fires if something has
    re-pointed one of them (an override, a stray env var, a second Verisim).
    It would not have caught t_05b48b69 on its own — that ingest used one host
    for both — but it makes a split-horizon source address loud instead of
    silent. hr.locations is static, so the counts are stable enough to compare.
    """
    resp = requests.get(f"{VERISIM_API_URL}/grocery/hr/locations", timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    api_n = len(payload if isinstance(payload, list) else payload.get("data", []))
    db_n, db_err = _source_counts(["hr.locations"])
    if db_err:
        raise RuntimeError(
            f"source DB at {VERISIM_DB['host']}:{VERISIM_DB['port']} does not expose "
            f"hr.locations: {db_err['hr.locations']}"
        )
    db_n = db_n["hr.locations"]
    if api_n != db_n:
        raise RuntimeError(
            f"source address mismatch: {VERISIM_API_URL}/grocery/hr/locations reports "
            f"{api_n} locations but the source DB at {VERISIM_DB['host']}:{VERISIM_DB['port']} "
            f"holds {db_n} — the API and the DB are different instances"
        )
    log.info(
        "[verify] source identity ok — %s and %s:%s agree (hr.locations=%d)",
        VERISIM_API_URL, VERISIM_DB["host"], VERISIM_DB["port"], api_n,
    )


def _reconcile(rows: list, src: dict) -> tuple:
    """Pure decision half of the invariant: classify reconciled rows.

    rows: [(task_id, raw_schema, raw_table, relation, raw_count_or_None)]
    src:  {source relation: source row count}

    Returns (excess, missing) where excess is
    [(task_id, raw relation, source relation, raw_count, source_count)] for every
    raw table holding MORE rows than its source, and missing lists source
    relations that could not be read (a broken mapping, not a data problem).
    Kept separate from the querying so the rule itself is unit-testable.
    """
    excess, missing = [], []
    for task_id, raw_schema, raw_table, relation, raw_n in rows:
        if src.get(relation) is None:
            missing.append(relation)
            continue
        if raw_n is None or raw_n <= src[relation]:
            continue
        excess.append(
            (task_id, f"{raw_schema}.{raw_table}", relation, raw_n, src[relation])
        )
    return excess, missing


def _excess_message(excess: list, rows_checked: int) -> str:
    detail = "; ".join(
        f"{raw_rel} has {raw_n} rows but {rel} holds only {src_n} ({raw_n / src_n:.1f}x)"
        if src_n else
        f"{raw_rel} has {raw_n} rows but {rel} is empty"
        for _, raw_rel, rel, raw_n, src_n in excess
    )
    return (
        f"raw tables hold rows that are not in the source ({len(excess)} of "
        f"{rows_checked} table(s)): {detail} — source API {VERISIM_API_URL}, source DB "
        f"{VERISIM_DB['host']}:{VERISIM_DB['port']}. The ingest is reading a "
        f"different instance than this stack owns, or appending instead of upserting."
    )


def verify_raw_vs_source(**context) -> None:
    """Fail the ingest when a raw table holds MORE rows than its source relation.

    The rule is one-sided on purpose. The source is live: it only ever grows
    while a run is in flight, so the source count read *after* the load is
    always >= the count that was available to fetch, and a faithful load can
    never exceed it. Re-reading a page cannot inflate a raw table either — the
    upsert is keyed on the primary key. An excess therefore means exactly one
    of:

      * the ingest read a different Verisim instance than the one this stack
        owns (t_05b48b69 — 1,136,360 foreign rows against a 98,112-row source),
      * the raw table was loaded from a source that was reloaded underneath it,
      * a future change broke the upsert back into a blind append.

    Shortfalls are logged but not failed: several endpoints legitimately serve a
    subset of their relation (SOURCE_PARTIAL), and a load interrupted by a live
    source is not an invariant violation. Partial loads are already fatal in
    _assert_complete, which compares rows written against the API's own total.
    """
    _assert_api_is_this_source()

    edw = _edw_conn()
    rows = []
    try:
        with edw.cursor() as cur:
            for cfg in TABLE_CONFIGS:
                task_id, raw_schema, raw_table = cfg[0], cfg[2], cfg[3]
                relation = SOURCE_RELATIONS[task_id]
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                """, [raw_schema, raw_table])
                raw_n = None
                if cur.fetchone()[0]:
                    cur.execute(f'SELECT COUNT(*) FROM "{raw_schema}"."{raw_table}"')
                    raw_n = cur.fetchone()[0]
                rows.append((task_id, raw_schema, raw_table, relation, raw_n))

        src, src_errors = _source_counts([r[3] for r in rows])
    finally:
        edw.close()

    log.info("[verify] raw vs source row counts (excess is the failure condition)")
    log.info("[verify]   %-32s %-34s %10s %10s %9s",
             "task", "source relation", "raw", "source", "delta")
    for task_id, _, _, relation, raw_n in rows:
        source_n = src.get(relation)
        if source_n is None or raw_n is None:
            log.info("[verify]   %-32s %-34s %10s %10s %9s", task_id, relation,
                     "ABSENT" if raw_n is None else raw_n,
                     "?" if source_n is None else source_n, "-")
            continue
        note = "  <- subset endpoint" if task_id in SOURCE_PARTIAL else ""
        log.info("[verify]   %-32s %-34s %10d %10d %+9d%s",
                 task_id, relation, raw_n, source_n, raw_n - source_n, note)

    excess, missing = _reconcile(rows, src)

    if missing:
        detail = "; ".join(
            f"{rel} ({src_errors.get(rel, 'not returned by the source')})"
            for rel in sorted(set(missing))
        )
        raise RuntimeError(
            f"reconciliation contract broken — {len(set(missing))} of {len(rows)} source "
            f"relation(s) unreadable at {VERISIM_DB['host']}:{VERISIM_DB['port']}: {detail}. "
            f"The source's schema is behind this DAG's table registry "
            f"(SOURCE_RELATIONS), or a mapping is wrong."
        )

    if excess:
        raise RuntimeError(_excess_message(excess, len(rows)))

    log.info("[verify] ok — no raw table exceeds its source relation (%d tables checked)",
             len(rows))


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
        "API-based ingestion grocery source → EDW raw (32 tables, 9 schemas). "
        "Alternative to Meltano tap-postgres. Pass {start_dt, end_dt} params "
        "for incremental backfill override."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    params={"start_dt": None, "end_dt": None},
    tags=["grocery", "api", "ingest", "granular"],
) as dag:

    ingest_tasks = []
    for schema, table_list in grouped.items():
        with TaskGroup(group_id=f"ingest_{schema}"):
            for (tid, api_path, raw_schema, raw_table, pk_col,
                 strategy, watermark_col, api_start_param, api_end_param) in table_list:
                ingest_tasks.append(PythonOperator(
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
                    execution_timeout=timedelta(minutes=60),
                ))

    # Runs whatever the ingest tasks did (all_done): the invariant exists to
    # fail loudly, so it must not be skipped because a sibling already failed.
    # trigger_rule is a string to avoid importing TriggerRule from a path that
    # moved between Airflow 2 and 3.
    verify = PythonOperator(
        task_id="verify_raw_vs_source",
        python_callable=verify_raw_vs_source,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=15),
    )
    ingest_tasks >> verify
