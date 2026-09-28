"""
Grocery Ingest — API Method DAG
================================
Ingests grocery source data from the Verisim HTTP API into the EDW raw schemas.
This is the sole ingestion path into the EDW (Meltano has been removed from the stack).

Strategy per table:
  - Full refresh: TRUNCATE raw table then reload all rows via paginated API calls.
  - Incremental: query MAX(watermark_col) from raw table, fetch records created
    after that timestamp. Falls back to 365 days ago on an empty table.

Read order: a route that references another relation is read BEFORE the route
that serves it (REFERENCING_ROUTES, wired as task dependencies below). Every
read is a snapshot of a live source, so a child route read after its parent can
pick up a generator tick the parent's window had already closed over — events
whose orders never landed, and a failing staging FK test on a healthy ingest.
The referenced route's window then ends where its referencing routes' reads ended
(`cut_from`), not at its own task start: both sides of a foreign key are judged
at one instant, so the tick that would otherwise fall between them cannot.

One pair needs a third thing on top of the read order and the cut, because its
rows MUTATE after they are inserted: an online order's `status` moves long after
its `created_at`, so a window anchored on the insert clock can neither see a
state change on a row the watermark passed (the row is never re-read) nor be
bounded by the child's read instant safely (an order excluded for having moved
after it would sit below the next insert-clock watermark, i.e. lost, not
delayed). Such a route is declared in `AS_OF_ROUTES`, anchored on the clock that
moves with the mutation, and bounded by the instant its referencing route read
to — `[MAX(<state clock>), the child's cut]`, both ends from one instant, so the
state the parent holds matches the snapshot boundary the child was read at.

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
PAGE_SIZE = 1000  # legacy default page size; the effective page is probed per route (_effective_limit)
PREFERRED_LIMIT = 10_000  # largest page this API accepts on most routes (422 above it, measured)
API_MAX_LIMIT = 1000  # every route accepts this page size — last resort for the probe
PAGE_OVERLAP_FRACTION = 20  # consecutive pages overlap by 1/N of the page size — a boundary trip-wire, not the completeness guarantee (see _fetch_pages)
SMALL_TABLE_THRESHOLD = API_MAX_LIMIT  # kept for compatibility: page size below which one request suffices
MAX_PAGES = 10_000  # safety cap: fail if pagination exceeds this (infinite loop guard for volatile endpoints)
DISTINCT_TRACK_MAX = 1_000_000  # primary keys held in memory to verify a load landed everything the source advertised
_PAGE_LIMIT_CACHE: dict = {}  # path -> largest `limit` the route accepts (probed once)
_WINDOW_CACHE: dict = {}  # path -> (start_param, end_param) the route accepts, or None
_ROUTE_PARAMS_CACHE: dict = {}  # path -> set of query parameters the route declares (None = spec unreadable)
WINDOW_FAR_FUTURE = "2100-01-01"  # end bound for a full load: history tables hold future-dated rows (schedules)
INCREMENTAL_FALLBACK_DAYS = 365  # lookback when raw table is empty
FETCH_RETRIES = 4  # attempts per page request before failing the task (no partial-load-on-error)
FETCH_RETRY_BACKOFF = 2.0  # seconds; exponential per attempt, capped at 30s

# ---------------------------------------------------------------------------
# Lookback for watermarks that are BACKDATING columns — currently none
# ---------------------------------------------------------------------------
# An incremental load's start bound is `MAX(watermark_col)` of the raw table.
# That is only sound when the source stamps the column with the *insert* time
# and never moves a row backwards. A column that DOES move backwards is a
# *backdating* watermark: the watermark a run holds is then its newest row,
# while the batch that inserted it carries most of its rows in the past, so
# every row a later batch backdates below the watermark is below the window of
# every subsequent run — forever, and nothing notices, because the run
# reconciliation only knows the *requested window's* advertised total (a row
# never inside the window was never advertised) and verify_raw_vs_source is
# one-sided. The fix for that shape is a bounded reach-back, listed per table
# in INCREMENTAL_LOOKBACK_DAYS below.
#
# The pair this mechanism was built for is off it as of t_b474c79e, and the
# history is worth keeping because it is what justifies the registry entries:
# `pos.returns.return_dt` is backdated by
# `verisim-grocery:/app/generator/models/returns.py` —
#
#     return_dt = txn_dt + timedelta(days=random.randint(AGE_MIN_DAYS=2, 21))
#     if return_dt > sim_dt: return_dt = sim_dt
#
# with `txn_dt` a transaction aged 2-14 days — so t_4788529f reached 21 days
# back of the watermark (the generator's own maximum offset) to reach the
# backdated half of a nightly batch. That was a stopgap: bounded, but it
# re-reads N days of `return_dt` density (~750 rows/day on the dev source,
# ~16k rows / ~16 requests a night) to catch a delta of a few hundred rows.
#
# Both return routes now expose the *insert* clock instead — `created_after` /
# `created_before` filtering `pos.returns.created_at` (`DEFAULT NOW()`, written
# by the same statement as the row), shipped as verisim `373cbe3` (t_5d2e2ab0)
# — so both tables watermark on `created_at` and the delta is the batch itself.
# The measured case for the switch, on the dev source's newest batch (80
# returns / 109 lines inserted 2026-09-21T07:08:27Z, `return_dt` spanning back
# to 2026-08-24): the old `[MAX(return_dt), now]` window reached 53/80 returns
# and 74/109 lines; the created_at window reached 80/80 and 109/109, with
# `total` matching SQL exactly (probe by verisim-dev, t_5d2e2ab0).
#
# What the entries empty at that switch, and why the mechanism stays: the shape
# recurs, and this is the only tested implementation of it.
# The recurrence is no longer hypothetical. `pos.transactions.transaction_dt`
# and `online.orders.placed_dt` are backdated the same way after any backfill
# (measured by verisim-dev on a fresh local seed: 89320 of 92741 transactions
# and 10699 of 11062 orders more than a day below the table's own last insert),
# and on the dev slot a POS gap-fill did land the losing rows — 10 transactions
# and 129 lines below raw's watermark, the source of the live dbt WARN
# `relationships_stg_pos_loyalty_point_transactions...`. That was fixed the same
# way as the returns (t_886f7d67) rather than by registering an entry here:
# every table this mechanism was built for is now on its route's insert clock,
# and a bounded window is a heal, not a guarantee — a reseed/backfill burst that
# backdates further than N in one wall-clock stretch lands below it, and is what
# the param-driven full reload is for (airflow/README.md → "Forcing a full
# reload"). Register one here only for a watermark column whose route still has
# no insert clock; the entry is config (watermark_col, api_start_param,
# api_end_param) and has to shift the *configured* bounds.
INCREMENTAL_LOOKBACK_DAYS: dict[str, int] = {}

# ---------------------------------------------------------------------------
# Single-writer lock — one loader per raw table, enforced in the database
# ---------------------------------------------------------------------------
# Every raw table is TRUNCATEd (or dropped) before it is refilled, so two loaders
# on one table destroy each other: the second empties what the first is filling,
# neither finishes, and anything sampling in between (a verify, an e2e gate) gets
# a verdict about a moving platform. That is what happened on 2026-09-21: a
# manual pipeline run and a second loader's rebuild worked raw_pos.transactions
# in the same minutes, and a gate run read the empty window as "raw empty-ish
# (0)" (t_657cebc3).
#
# `max_active_runs=1` on this DAG only keeps two DagRuns of *this DAG on this
# scheduler* apart. It cannot see a hand-run loader, a second Airflow aimed at
# the same EDW, an `airflow tasks test` run, or the documented raw-layer
# recovery (`drop schema raw_* cascade`). So the invariant lives where every
# writer has to pass: PostgreSQL.
#
# Each ingest task takes a session-level advisory lock on its own table and holds
# it for the whole task (acquire → ensure table → truncate → page-load → release
# when the connection closes). Same table: the second loader waits, then fails
# loudly with the holder named. Different tables: no interaction at all, so pool
# concurrency still buys throughput.
INGEST_LOCK_NAMESPACE = 0x646C6974  # 'dlit' — advisory-lock class for this loader
INGEST_LOCK_WAIT_S = 900  # seconds a task waits for a competing loader before failing
INGEST_LOCK_POLL_S = 5  # seconds between pg_try_advisory_lock attempts

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

    # Incremental on changed_at since t_34d4d575. price_history is an append-only
    # price-change log — models/pos.py INSERTs a row on every price change with
    # changed_at defaulting to NOW(), and nothing updates or deletes it — so the
    # watermark is monotone in insert order and immutable afterwards, which is
    # what an incremental load needs. Measured on dev 2026-09-21: 7455 rows /
    # 8 page requests / 1.2 s in the 06:28 run became 65 rows in 1 request /
    # 0.2 s in the 06:44 run (7 requests counting the probes).
    # It also removes a gap a truncate-and-reload leaves behind: this table grows
    # continuously (~50 rows in the 16 minutes between the 06:28 and 06:44 runs),
    # and the 06:28 full reload had already lost the ones generated while it ran —
    # raw held 7455 of the source's 7500, and re-reading the next run's snapshot
    # cannot recover them either. The `>=` window does: after the 06:44 run the
    # raw table held every key the source had at the watermark, 0 missing, 0 stale.
    # Trade-off (see the online_order_items entry): a missed delta is not healed
    # by the next run. Force the history back in with the DAG params — the
    # recipe is in airflow/README.md ("Forcing a full reload").
    ("pos_price_history", "/grocery/pos/price-history",
     "raw_pos", "price_history", "price_history_id",
     "incremental", "changed_at", "start_dt", "end_dt"),

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

    # Incremental on the INSERT clock since t_886f7d67 (source side: verisim
    # `4ec85ec`, card t_6d2ebc52) — the same switch as the return tables above,
    # for the same reason, on the table that motivated the whole mechanism.
    # `transaction_dt` is BACKDATED by the source's backfill path: `main.py`
    # replays a day hour by hour and calls `pos.generate_pos_transactions(...,
    # sim_dt=hour boundary)`, so a whole hour's batch is INSERTed at tick time
    # carrying the hour it belongs to. Measured on the dev slot before this
    # change: source 95031 transactions against raw 95009, of which 10 sat
    # at-or-below raw's own MAX(transaction_dt)
    # (2026-09-21T03:24:18.390145-04) and no `[watermark, now]` window could
    # ever reach them — the gap-fill batches inserted at 03:19:17 and 03:50:34,
    # each carrying `transaction_dt` 00:00/01:00/02:00 — plus 129 of that
    # table's 550069 lines. One of the 10
    # (`837b2ab3-8ccd-4ebc-88ad-cf93dfed8ce2`) is the live dbt WARN
    # `relationships_stg_pos_loyalty_point_transactions...`: the loyalty-point
    # row is on the insert clock and loaded fine, the transaction it references
    # was not (t_b474c79e filed this, rather than fixing it there).
    #
    # Healed *before* the switch, by a one-off `full` run on this entry
    # (truncate + re-read the whole relation: an exact mirror, per entry)
    # rather than by the param-driven recipe, which applies its 100-year window
    # to every incremental table in the run. After the heal: 95031 of 95031
    # rows key-for-key against the source, 0 missing / 0 stale, and `created_at`
    # present for every row (the payload auto-ALTER in `_detect_schema_drift`
    # adds it as TEXT) — which is why the switch's own transition branch did not
    # fire for this pair.
    #
    # Caveat, unchanged in shape: a params-driven run reads the hardcoded
    # `params_conf["start_dt"]/["end_dt"]` keys and now applies them to
    # `created_after`/`created_before` (harmless for the documented
    # `2000-01-01 → 2100-01-01` full reload). `/grocery/pos/transactions` used to
    # *require* `start_dt`/`end_dt`; verisim `4ec85ec` relaxed both to optional,
    # which is what made this swap a two-field change instead of a 422.
    ("pos_transactions", "/grocery/pos/transactions",
     "raw_pos", "transactions", "transaction_id",
     "incremental", "created_at", "created_after", "created_before"),

    # Same switch, same window (t_886f7d67). The route returns the header's
    # `created_at` with every line: a transaction line has no timestamp of its
    # own, so the only clock it used to inherit was the backdated
    # `transaction_dt` — and the 129 lines that were missing from raw belonged
    # to the 10 unreachable transactions above. The column reaches the raw table
    # with the first load of this route (`_detect_schema_drift` ALTERs it in as
    # TEXT); here the heal delivered it, so this entry watermarks on it from its
    # first post-switch run.
    ("pos_transaction_items", "/grocery/pos/transaction-items",
     "raw_pos", "transaction_items", "item_id",
     "incremental", "created_at", "created_after", "created_before"),

    # Incremental on the INSERT clock since t_b474c79e (source side: verisim
    # `373cbe3`, card t_5d2e2ab0). `created_after`/`created_before` filter
    # `pos.returns.created_at` — `DEFAULT NOW()`, written by the same statement
    # as the row — so the watermark is monotone in insert order and immutable
    # afterwards, and the window's delta is the batch itself. That is the
    # property `return_dt` does not have: the route still offers it as
    # `start_dt`/`end_dt`, and it is still backdated by the generator
    # (`return_dt = txn_dt + randint(2, 21)`, clamped), which is why the
    # watermark moved off it — a `[MAX(return_dt), now]` window reached 53 of
    # the 80 returns in the source's 2026-09-21T07:08:27Z batch, and the 21-day
    # lookback t_4788529f put under it re-read ~16k rows a night to reach them.
    # Read INCREMENTAL_LOOKBACK_DAYS' comment before reaching for that shape
    # again: it is empty now, and this is why.
    #
    # Known caveat, read off the code and pinned by the suite rather than
    # discovered in production: the DAG-param branch reads the hardcoded
    # `params_conf["start_dt"]/["end_dt"]` keys, so a *params-driven*
    # run of this table applies those two values to `created_after`/
    # `created_before` — i.e. it bounds the insert clock, not `return_dt`.
    # Harmless for the documented full reload (`2000-01-01 → 2100-01-01` covers
    # every row that exists, and `_fetch_all` bisects the window), but it is not
    # a caller-supplied created_at window and it no longer means what it meant
    # for the `return_dt` tables. A narrow historical `return_dt` window is a
    # `full`-strategy load, not a params run of this entry. Verisim offered a
    # params-style created_at window on request (t_5d2e2ab0); until then this is
    # the shape.
    ("pos_returns", "/grocery/pos/returns",
     "raw_pos", "returns", "return_id",
     "incremental", "created_at", "created_after", "created_before"),

    # Same switch, same window (t_b474c79e). The route returns `r.created_at`
    # with every line — added in verisim `373cbe3` for exactly this reason: a
    # return line has no timestamp of its own, and the only time it used to
    # inherit was the header's backdated `return_dt`. The column reaches the raw
    # table with the first load of this route (the payload column auto-ALTER in
    # `_detect_schema_drift` adds it as TEXT), and `_get_watermark` can only read
    # it from the run *after* that one — see the missing-watermark branch in
    # `ingest_table`, which is the transition this entry took.
    ("pos_return_items", "/grocery/pos/return-items",
     "raw_pos", "return_items", "return_item_id",
     "incremental", "created_at", "created_after", "created_before"),

    # ── Online (e-commerce orders, t_24fae529) ──────────────────────────────
    # History first, because it is what justifies each step, then the entry.
    # It was incremental on the INSERT clock from t_886f7d67 (source side:
    # verisim `4ec85ec`, card t_6d2ebc52) — the same switch as the POS pair
    # above, taken off `placed_dt`, the watermark from t_34d4d575. `placed_dt` is
    # the tick time in steady state, and that is exactly what made it look safe.
    # It is not: the source's
    # backfill path replays a day hour by hour (`main.py` →
    # `online.generate_online_orders(conn, cfg, sim_dt, ...)` with `sim_dt` at
    # the hour boundary, and the INSERT writes `placed_dt = sim_dt`), so a
    # gap-fill lands orders *below* MAX(placed_dt) and a `[watermark, now]`
    # window loses them permanently — the run reconciliation only knows the
    # requested window's advertised total, and `verify_raw_vs_source` fails on
    # excess only.
    # `created_at` is `DEFAULT NOW()`, written by the same statement as the row:
    # monotone in insert order and immutable, so the delta is the batch itself.
    # Measured before that switch on the dev slot: this table had NO such gap
    # (11080 orders / 210114 lines, key-for-key identical to the source), so
    # nothing was healed here and this is the pair that took the transition
    # branch (no `created_at` column in raw yet → bounded fallback + WARNING,
    # then watermark) rather than being healed first. The insert clock was the
    # right fix for the backdating defect; it is simply not the right clock for a
    # row whose STATE moves, which is the next switch.
    # Watermark moved from the insert clock to the STATE clock in t_5a16129f:
    # `updated_after`/`updated_before` on `updated_at`, which verisim `e9bd295`
    # (card t_51bbc12e) added to the payload and the window. Two defects, one
    # cause — the insert clock cannot express "as of":
    #
    #   * it cannot see a state change. `created_at` is fixed at insert, so once
    #     a newer order has moved the watermark past an older row, that row is
    #     never re-read: an order loaded as 'placed' keeps that status in raw
    #     forever. Nothing asserted on it (the reconcile test looks for completed
    #     orders, not stale ones), and the events route being a FULL reload hid
    #     it — the state was as fresh as the last full read of the pair.
    #   * it cannot be bounded safely. The completion residual (t_7e427ee6 →
    #     t_5a16129f) is an order that completes *while this route is being
    #     read*: it satisfies `created_before=<cut>` (it was created long before)
    #     and lands as 'completed' while its terminal event, inserted after the
    #     cut, is not read until the next run — 5 rows on the test slot's
    #     2026-09-21T21:22:29Z fresh seed, healed by the next events read, which
    #     is what made it look like a flake. Excluding it needs the cut on the
    #     end bound; doing that on the insert clock loses the row instead of
    #     delaying it (its `created_at` is below the next watermark).
    #
    # `updated_at` is what a watermark needs: `DEFAULT NOW()`, written by the
    # same statement that writes the row, and bumped by every lifecycle step —
    # so it is monotone in state-change order and immutable between changes.
    # That it moves with the STATE and not only with inserts is what makes the
    # delta complete: an old row whose status just changed is picked up, which
    # is the answer to the first bullet above.
    #
    # And it is the child's clock for the same row: the generator stamps a status
    # change and the event that explains it in ONE transaction, so
    # `updated_at == MAX(online.order_events.created_at)` for every order —
    # measured on the dev slot 2026-09-21: 39243 of 39243 orders, 0 differing.
    # verisim `80b3e0f` pins that source-side; without it this switch would be
    # unsound, because a transition that did not bump `updated_at` would be
    # invisible to every subsequent run. That equality is what makes the window
    # `[MAX(updated_at) in raw, the events route's read cut]` (AS_OF_ROUTES)
    # safe: no order is loaded ahead of the event that explains it, and none is
    # left behind.
    #
    # The route still accepts `created_after`/`created_before` (`created_at`);
    # this config deliberately does not use them. `updated_*` is the load's
    # window, and a DAG-param run applies `start_dt` to `updated_after` and the
    # child's cut to `updated_before` (see the as-of branch in `ingest_table`).
    ("online_orders", "/grocery/online/orders",
     "raw_online", "orders", "order_id",
     "incremental", "updated_at", "updated_after", "updated_before"),

    # Incremental since t_34d4d575, on the insert clock since t_886f7d67 (the
    # same switch as its header — the line has no timestamp of its own, so the
    # route joins `o.created_at` from `online.orders` and returns it per row).
    #
    # Why the table went incremental at all (t_34d4d575, still true): before
    # verisim ee9c73a (t_d7892e10) the route offered no window at all, so a
    # whole-table reload was the only load available (t_7c88f2f9) — 210114 rows,
    # the largest raw table in the EDW. The header+line pair is INSERTed in one
    # transaction; the lifecycle only moves the *header*'s status and never
    # touches `placed_dt`/`created_at`.
    #
    # Trade-off, stated because it is not free: each full reload healed any past
    # gap on every run; this load does not, and a missed delta stays missed.
    # Two things keep that honest — the run reconciliation still fails the task
    # when the requested window advertises more rows than landed, and
    # verify_raw_vs_source still fails when the raw table holds rows the source
    # does not (a reseeded source under a populated raw table is the case a
    # watermark load cannot see). To put the whole history back in, pass the DAG
    # params; the recipe and its measured cost are in airflow/README.md →
    # "Forcing a full reload" (measured 2026-09-21: 265 requests / 36.9 s for this
    # table, and the whole ingest 1691 requests / 117.7 s).
    #
    # Its window END is not its own task start since t_5a16129f: it is
    # `online_order_events`'s read instant (SNAPSHOT_BOUND_ROUTES), so the whole
    # online cluster — the events, these lines, and the order they belong to — is
    # judged at one instant. The clock and the watermark are unchanged: `created_at`
    # is still what this route loads and watermarks on, and a line inserted after
    # the instant is read by the next run.
    ("online_order_items", "/grocery/online/order-items",
     "raw_online", "order_items", "item_id",
     "incremental", "created_at", "created_after", "created_before"),

    # Incremental on the INSERT clock since t_5a16129f (source side: verisim
    # `e9bd295`, card t_51bbc12e), where it used to be a FULL reload.
    #
    # There was no clock to watermark on before that, and the reason is worth
    # keeping: `online.order_events.created_at` has existed since the online
    # channel landed (`0cfd0ed`) and the generator writes it (`DEFAULT NOW()`,
    # same statement as the row) — what was missing was the ROUTE: `/grocery/
    # online/order-events` declared no window on it and returned no such column,
    # so a configured window was silently ignored and a full reload was the only
    # load the source offered (measured 2026-09-21: 184165 rows on the dev slot,
    # 60797 on the test slot's fresh seed).
    #
    # That cost more than bytes. A `full` route ignores the read cut (it reads
    # the whole table, so its cut is its last read rather than an instant both
    # windows can end at), so the read order t_7e427ee6 put in place was only
    # half wired for this pair: the events route read past the instant the orders
    # window closed at, and an order completing inside that gap landed as
    # 'completed' with its event still unread. Windowed, this route's cut IS the
    # instant the orders route is bounded by (AS_OF_ROUTES), so the two routes
    # are judged at one point in time instead of at two.
    #
    # `created_at` is `DEFAULT NOW()`, written by the same statement as the row:
    # monotone in insert order and immutable, the property a watermark needs.
    # No `created_at` column is in the raw table yet (this route has never
    # returned one), so the FIRST run with this config takes the
    # missing-watermark transition branch in `ingest_table` — a bounded fallback
    # window with a WARNING naming the column, which is also the load that
    # delivers the column via the payload ALTER in `_detect_schema_drift`. The
    # watermark is used from the run after that one. On a fresh seed the table is
    # empty, so the other fallback branch fires instead and the column arrives
    # with the first payload; either way the history is inside the 365-day
    # horizon (the source seeds ~30 days).
    ("online_order_events", "/grocery/online/order-events",
     "raw_online", "order_events", "event_id",
     "incremental", "created_at", "created_after", "created_before"),

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


# ---------------------------------------------------------------------------
# Read order — a route that references another relation is read BEFORE it
# ---------------------------------------------------------------------------
# Every route is read independently and each read is a point-in-time snapshot of
# a live source, so a row can only carry a foreign key whose parent was already
# committed when that route was read. Read the child AFTER the parent and it can
# pick up a generator tick the parent's window had already closed over: the
# events land, their orders never do, and dbt's `relationships` test fails the
# pipeline on a perfectly healthy ingest.
#
# Measured on the dev slot, 2026-09-21 (the dev instance ticks every 30 s):
#   online_orders        14:00:05.98 → 14:00:10.27   incremental, window ends at
#                                                    its own task start (now_iso)
#   online_order_events  14:00:09.72 → 14:00:28.90   FULL reload — no window, so
#                                                    it pages past its own start
# The events route began 3.7 s after the orders route and reloaded 145k rows for
# 19 s, so it read ticks at 14:00:13 and 14:00:27 that the orders window (closed
# at 14:00:05.98) could not see. Result: 8 staged events with no staged order and
# `relationships_stg_online_order_events_order_id__order_id__ref_stg_online_orders_`
# red on a fresh seed — nightly failure, and mild because marts still build
# (t_7e427ee6; on the test slot the same shape was 10 events at 20:04:13.717).
#
# Reading the child FIRST closes it: the parent's read then ends after everything
# the child could have seen, so every parent the child loaded is inside the
# parent's window — its upper bound is that later read, and its lower bound is
# either `MAX(watermark)` in raw (above which every unseen row necessarily sits:
# an unloaded parent's insert clock is newer than every loaded one, or the
# incremental invariant that made the watermark sound was already broken) or, for
# a `full` reload, a fresh mirror of the source. The reverse skew — a parent with
# no child yet — is the benign direction; the next run delivers the child.
#
# The upper bound is not the parent's own task start though: that is a scheduling
# gap *later* than the child's read, and it makes the foreign key hold by
# accident of scheduling rather than by construction. A parent's window ends
# where its children's reads ended (`cut_from` in `ingest_table`) — the latest of
# them, so it still reads past everything that references it.
#
# The list is exactly the cross-route `relationships` tests in
# airflow/dbt/grocery/models/staging/staging.yml, master data included — the
# generator also hires employees, adds loyalty members and re-seeds coupons
# mid-run, so a master read early is stale in the same way. `dags/tests/
# test_load_order.py` fails when this list drifts from that suite, which is
# deliberate: the FK test is only assertable because this read order makes it
# true. Add a dbt FK test, declare its read order here.
REFERENCING_ROUTES = [
    # child route (read first)          parent route (read after)
    # ── POS ─────────────────────────────────────────────────────────────────
    ("pos_transaction_items",           "pos_transactions"),
    ("pos_loyalty_point_transactions",  "pos_transactions"),
    ("pos_returns",                     "pos_transactions"),
    ("pos_return_items",                "pos_returns"),
    ("pos_return_items",                "pos_transaction_items"),
    # The promotions a line was discounted by (coupon_id / deal_id, surfaced on
    # stg_pos_transaction_items by t_f0bdffaf). Child first, as everywhere else:
    # the items route reads, then each promo route reloads its table, so every
    # coupon/deal those lines reference is in raw before the lines are staged.
    # Both promo routes are `full` mirrors of an `active_only` API route
    # (SOURCE_PARTIAL), which is why the dbt side of these two edges is
    # `severity: warn`: a promo deactivated after it was used legitimately drops
    # out of the source snapshot while the items that reference it stay — a real
    # gap in the source, not a defect in the model. Declaring the edges is what
    # leaves that warn measuring the source's gap and nothing else; undeclared,
    # whether the parent is in raw at all is scheduling luck (t_27132746, on the
    # FK tests 1c9a833 added).
    ("pos_transaction_items",           "pos_coupons"),
    ("pos_transaction_items",           "pos_combo_deals"),
    ("pos_products",                    "pos_departments"),
    ("pos_loyalty_point_transactions",  "pos_loyalty_members"),
    # ── Online ──────────────────────────────────────────────────────────────
    ("online_order_items",              "online_orders"),
    ("online_order_events",             "online_orders"),
    # ── Ordering → fulfillment → transport ──────────────────────────────────
    ("ordering_store_order_items",      "ordering_store_orders"),
    ("fulfillment_orders",              "ordering_store_orders"),
    ("fulfillment_items",               "fulfillment_orders"),
    ("transport_load_items",            "transport_loads"),
    ("transport_load_items",            "fulfillment_orders"),
    ("transport_load_items",            "ordering_store_orders"),
    ("transport_loads",                 "transport_trucks"),
    # ── Inventory / pricing ─────────────────────────────────────────────────
    ("inv_receipt_items",               "inv_receipts"),
    ("inv_products",                    "pos_products"),
    ("inv_stock_levels",                "pos_products"),
    ("inv_receipt_items",               "pos_products"),
    ("inv_shrinkage_events",            "pos_products"),
    ("pricing_ad_items",                "pricing_weekly_ads"),
    ("pricing_ad_items",                "pos_products"),
    # ── Product / item rows reference the products they were sold as ────────
    ("pos_transaction_items",           "pos_products"),
    ("online_order_items",              "pos_products"),
    ("ordering_store_order_items",      "pos_products"),
    ("fulfillment_items",               "pos_products"),
    # ── Master data those routes reference ──────────────────────────────────
    ("hr_employees",                    "hr_locations"),
    ("hr_schedules",                    "hr_employees"),
    ("hr_schedules",                    "hr_locations"),
    ("timeclock_events",                "hr_employees"),
    ("timeclock_events",                "hr_locations"),
    ("pos_transactions",                "hr_employees"),
    ("pos_transactions",                "hr_locations"),
    ("pos_returns",                     "hr_locations"),
    ("online_orders",                   "hr_locations"),
    ("ordering_store_orders",           "hr_locations"),
    ("fulfillment_orders",              "hr_locations"),
    ("transport_loads",                 "hr_employees"),
    ("transport_loads",                 "hr_locations"),
    ("inv_stock_levels",                "hr_locations"),
    ("inv_receipts",                    "hr_locations"),
    ("inv_shrinkage_events",            "hr_locations"),
]

_routes = {c[0] for c in TABLE_CONFIGS}
_unknown_routes = sorted({t for pair in REFERENCING_ROUTES for t in pair} - _routes)
if _unknown_routes:
    raise ValueError(
        "REFERENCING_ROUTES names task(s) that TABLE_CONFIGS does not declare: "
        + ", ".join(_unknown_routes)
    )


# ---------------------------------------------------------------------------
# As-of routes — a referenced route whose rows MUTATE after they are inserted
# ---------------------------------------------------------------------------
# The read cut above is enough when a row's state is fixed at insert: an event
# read by instant B has a parent whose insert clock is inside the parent's
# `[MAX(created_at), B]` window, so whichever run loads the parent, both sides of
# the foreign key agree.
#
# It is not enough for `online_orders`. The generator mutates that row's status
# long after the insert (placed → confirmed → picking → picked_up/delivered), so
# there is a state to be "as of" that an insert clock cannot express, and the
# pair fails in both directions on a live source:
#
#   * `assert_online_orders_reconcile` — an order that completes *while the
#     orders route is being read* is 'completed' in the row and its terminal
#     event is not in the events read (that read finished at B, the order moved
#     after B). 5 rows on the test slot's 2026-09-21T21:22:29Z fresh seed; the
#     next events read healed it, which is what made it look like a flake. This
#     is the residual t_7e427ee6 filed and t_5a16129f closes.
#   * the mirror image: a `created_at` watermark moves past an old row and that
#     row is never re-read, so a status change on it stays wrong in raw forever.
#
# So a route like that is declared here, with the clock that moves when the row's
# state does, and is read as `[MAX(<state clock> in raw), the instant its
# referencing route read to]`:
#
#   * the LOWER bound is the state clock, so a status change is a delta like any
#     other and no row is ever left behind (the second bullet above);
#   * the UPPER bound is the cut of the route named in `bounded_by` — that
#     route's own window end, i.e. one instant captured before either route read
#     — so the parent holds no state newer than the child's snapshot (the first
#     bullet).
#
# The provider is named rather than "the latest of my children's cuts": the pair
# is only judged at one instant if the bound IS the child's window end. Taking
# the max over the children (what a plain `cut_from` route does) would let a
# sibling child that started a second later push the bound past the window the
# events were actually read in — reintroducing exactly the residual, in miniature.
#
# The state clock has to be what a watermark needs — written in the same statement
# as the row, monotone, immutable between changes — and, for the bound to mean
# what it says, equal to the CHILD's clock for that row: the generator writes a
# status change and the event that explains it in one transaction, so
# `updated_at == MAX(online.order_events.created_at)` per order (measured on the
# dev slot 2026-09-21: 39243 of 39243 orders, 0 differing). verisim `80b3e0f`
# (card t_51bbc12e) pins that source-side — a transition that stopped bumping
# `updated_at` would make this window silently incomplete.
#
# `dags/tests/test_load_order.py` (section 5) and
# `dags/tests/test_incremental_watermarks.py` (section 5) fail when this
# declaration, its `TABLE_CONFIGS` entry, `REFERENCING_ROUTES` and the DAG wiring
# drift apart.
AS_OF_ROUTES = {
    # route whose rows mutate  ->  the clock that moves with the mutation, the
    # window bounds it must be configured with, and the route whose read instant
    # bounds it (which must be read FIRST — see REFERENCING_ROUTES)
    "online_orders": {
        "state_clock": "updated_at",
        "bounds": ("updated_after", "updated_before"),
        "bounded_by": "online_order_events",
    },
}

# A route whose window has to end at ANOTHER route's read instant without
# referencing it. An as-of pair is one cluster judged at one instant (see above);
# every route in that cluster ends its window there, whatever clock it watermarks
# on.
#
# Only `online_order_items` needs this, and the reason is the FK it carries INTO
# the cluster. The cluster's instant is `online_order_events`'s window end (the
# state route's own instant, `AS_OF_ROUTES`). `online_orders` ends there, so an
# item route ending at its own task start — a few seconds later — can hand over a
# row the orders window cannot see: an order INSERTed after the instant, in the
# same transaction as the item that references it. The item is loaded, its order is
# not, and no later run re-reads it — an insert clock does not look back. That is
# the orphan class t_7e427ee6 closed by ordering the reads, arriving through the
# one edge whose bound is a specific instant rather than the latest child's read.
# (It held on the dev slot by accident of task creation order — items is declared
# before events, so it started first. Accident is what t_7e427ee6's comment calls
# out as not good enough.)
#
# Ending at the instant instead is safe in the other direction: this route
# watermarks on `created_at`, immutable and monotone, so an item inserted between
# the instant and its own read is DELAYED to the next run — its insert clock is
# above the next watermark — rather than lost. Same property that lets every other
# insert-clock route end at a cut.
#
# Wired like a `cut_from` edge (contract in the route's own config, edge in the
# DAG body, read order asserted): `dags/tests/test_load_order.py` 6a–6d.
SNAPSHOT_BOUND_ROUTES = {
    # route that ends at an instant  ->  the route whose read instant it is
    "online_order_items": "online_order_events",
}

_unknown_instant_routes = sorted(
    (set(SNAPSHOT_BOUND_ROUTES) | set(SNAPSHOT_BOUND_ROUTES.values())) - _routes
)
if _unknown_instant_routes:
    raise ValueError(
        "SNAPSHOT_BOUND_ROUTES names task(s) that TABLE_CONFIGS does not "
        "declare: " + ", ".join(_unknown_instant_routes)
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


def _parse_window(val: str):
    """Parse an API window bound: date-only ('2026-09-21') or ISO ('...T00:00-04:00')."""
    if len(val) == 10 and val[4] == "-" and val[7] == "-":
        return datetime.strptime(val, "%Y-%m-%d"), True
    return datetime.fromisoformat(val), False


def _fmt_param(val: str, param: str | None) -> str:
    """Format a bound for the endpoint: `*_date` params take YYYY-MM-DD, others ISO."""
    return val[:10] if param and param.endswith("_date") else val


def _fmt_window(dt: datetime, date_only: bool) -> str:
    return dt.strftime("%Y-%m-%d") if date_only else dt.isoformat()


def _effective_limit(path: str, params: dict | None = None) -> int:
    """Largest `limit` this endpoint accepts — probed once per route, then cached.

    The DAG used to hard-code a 1000-row cap (a comment claimed the API returned
    422 above it). It does not: most routes accept 10000 and a few accept 5000
    (`/grocery/pos/loyalty-members`), while 20000 is refused. Fewer, larger pages
    mean fewer requests, and they also mean fewer of the overlap repeats that
    `_fetch_pages` deliberately buys; for a table that fits in one page they remove
    paging entirely.

    t_7c88f2f9 also justified this as "10x fewer chances for the source's unstable
    ORDER BY to lose a row". That half of the reason is gone — the source's ORDER
    BY is a total order as of verisim t_d7892e10 and paging is complete by
    construction (see _fetch_pages) — but the request saving stands on its own, so
    the probe stays.

    The probe carries the caller's filter parameters: routes whose window is a
    required parameter used to answer 422 to a bare `limit`/`offset` request,
    which would otherwise look like "this route accepts no page size above
    1000". No route requires a window any more — `/grocery/pos/transactions`
    was the last one and both of its bounds became optional in verisim
    `4ec85ec` (t_6d2ebc52) — but the reason to pass them is unchanged: the
    window is part of what the route accepts, and the page size is probed
    against the request the loader will actually make.
    """
    if path in _PAGE_LIMIT_CACHE:
        return _PAGE_LIMIT_CACHE[path]
    base = dict(params or {})
    for limit in (PREFERRED_LIMIT, 5000, API_MAX_LIMIT):
        try:
            resp = _request_page(f"{VERISIM_API_URL}{path}",
                                 {**base, "limit": limit, "offset": 0}, 120, path)
        except Exception:                                            # noqa: BLE001
            break  # transport trouble: fall through to the conservative page size
        if resp.status_code < 400:
            log.info("%s: page size %d accepted (using it for every request)", path, limit)
            _PAGE_LIMIT_CACHE[path] = limit
            return limit
    log.warning("%s: no probed page size accepted — falling back to %d rows/request",
                path, API_MAX_LIMIT)
    _PAGE_LIMIT_CACHE[path] = API_MAX_LIMIT
    return API_MAX_LIMIT


def _spec_path_matches(spec_path: str, request_path: str) -> bool:
    """Does OpenAPI path `spec_path` serve `request_path`?

    Most grocery routes are declared with an industry placeholder
    (`/{industry}/pos/transactions`) while the ingest calls the concrete form
    (`/grocery/pos/transactions`), so an exact string compare finds nothing and
    marks a perfectly good window as unsupported.
    """
    spec_parts = spec_path.strip("/").split("/")
    req_parts = request_path.strip("/").split("/")
    if len(spec_parts) != len(req_parts):
        return False
    return all(part == req or (part.startswith("{") and part.endswith("}"))
               for part, req in zip(spec_parts, req_parts))


def _route_query_params(path: str) -> set | None:
    """Query parameters this route declares, from the API's own OpenAPI document.

    Matches concrete and templated spec paths. Cached per path. Returns None when
    the document cannot be read, which callers treat as "unknown" (trust the
    config) rather than "no parameters" (do not).
    """
    if path in _ROUTE_PARAMS_CACHE:
        return _ROUTE_PARAMS_CACHE[path]
    names: set | None = None
    try:
        resp = _request_page(f"{VERISIM_API_URL}/openapi.json", {}, 60, "/openapi.json")
        resp.raise_for_status()
        names = set()
        for spec_path, methods in resp.json().get("paths", {}).items():
            if not _spec_path_matches(spec_path, path):
                continue
            for method in methods.values():
                if not isinstance(method, dict):
                    continue
                names |= {p.get("name") for p in method.get("parameters", [])
                          if p.get("in") == "query"}
    except Exception as exc:                                         # noqa: BLE001
        log.info("%s: could not read the OpenAPI spec (%s)", path, exc)
    _ROUTE_PARAMS_CACHE[path] = names
    return names


def _discover_window(path: str) -> tuple[str, str] | None:
    """Window parameters this route accepts even though TABLE_CONFIGS names none.

    Read from the API's own OpenAPI document and cached per path. Several routes
    take start/end filters the ingest config does not use — `/grocery/hr/schedules`
    takes `start_date`/`end_date` and `/grocery/online/order-events` takes
    `start_dt`/`end_dt`.

    Status after t_4f3f46fd. t_7c88f2f9 introduced this to keep *whole-table* loads
    complete: the source's views then paged with `ORDER BY <non-unique column>
    LIMIT/OFFSET`, so fetching a window in one request was the only way to load
    those routes without loss (hr_schedules lost 5 of 2094 rows, order-events 271
    of 59161). The source fixed that ordering (verisim t_d7892e10, which also gave
    this route family its PK tiebreakers), so the premise was re-measured on the
    fixed build from inside this DAG: plain consecutive offsets at limit=1000 with
    **no** overlap returned exactly `total` distinct primary keys on the first
    pass — 23 walks, five routes, zero shortfalls, identical second passes. A
    whole-table route therefore pages now and this function is not consulted for
    it; and if one is ever promoted here again by accident, `ingest_table` logs
    the decision instead of taking it silently. The cost of the old behaviour is
    what settled it: on `/grocery/online/order-items` the bisection spent 1153
    HTTP requests over 210114 rows against ~85 for the pager.

    It remains reachable for an `incremental` route that does not declare its own
    window — there the window is the load's semantics (the watermark delta), not a
    way around a page boundary.

    Returns None when the route has no such pair (or the spec cannot be read) —
    callers then page and rely on the load reconciliation.
    """
    if path in _WINDOW_CACHE:
        return _WINDOW_CACHE[path]
    found: tuple[str, str] | None = None
    names = _route_query_params(path)
    if names:
        for pair in (("start_dt", "end_dt"), ("start_date", "end_date")):
            if set(pair) <= names:
                found = pair
                break
    _WINDOW_CACHE[path] = found
    return found


def _probe_window(path: str, params: dict) -> int | None:
    """How many rows the API says this filter holds — one cheap request (limit=1).

    Returns None when the endpoint does not report a `total` (nothing to verify
    against, and no way to size a split).
    """
    url = f"{VERISIM_API_URL}{path}"
    resp = _request_page(url, {**params, "limit": 1, "offset": 0}, 60, path)
    resp.raise_for_status()
    data = resp.json()
    total = data.get("total") if isinstance(data, dict) else None
    return total if isinstance(total, int) else None


def _fetch_all(path: str, params: dict, start_key: str, end_key: str,
               single_limit: int | None = None):
    """Yield every row matching `params` without ever paging inside a window.

    Why this exists (t_7c88f2f9): the source views page with
    `ORDER BY <non-unique column> LIMIT %s OFFSET %s` — e.g. `ORDER BY e.event_dt
    DESC` for timeclock/order events. Over a non-unique sort key Postgres may
    order tied rows differently per query, so the same row can land in two pages
    (and another row in neither). Measured on the dev source with the ingest's own
    window and limit=1000: `/grocery/online/order-events` reported total=59161 and
    returned 59161 rows in 60 pages, but only 58890 of them were distinct — 271
    rows arrived twice and 271 distinct rows were never returned by any page. The
    PK upsert absorbed the duplicates and the row count still matched `total`, so
    the raw table silently ended up 271 rows short (delivered -14, picked_up -6,
    confirmed -107, ... — exactly the 271), which surfaced downstream as 19 orders
    with no terminal event and one unpaired clock-in.

    Fetching a window in a single request (offset=0) has no boundary to cross, so
    it cannot lose a row. Where a window is larger than one response can carry,
    split it into `ceil(total / limit)` sub-windows and recurse; each leaf is
    fetched whole. Termination: every split strictly shrinks the window, and a
    window that cannot shrink further (0-width, or day granularity for a
    `*_date` parameter) falls back to the offset pager with a warning.

    Status after t_4f3f46fd. The source's ORDER BY is now a total order (verisim
    t_d7892e10), so this is no longer the only way to load a windowed route
    completely — plain offset paging reaches `total` distinct rows on the first
    pass (see _fetch_pages). The function is kept unchanged: it is still the right
    shape for a *filtered* window, which is what every route whose config declares
    a window uses (one request when the window fits, no page boundaries at all),
    and its last-resort fallback is no longer a risk. What changed is who reaches
    it: a whole-table route that merely *accepts* a window no longer bisects one
    (`ingest_table` gates _discover_window to non-`full` strategies). That gating
    is where the cost was — `ceil(total/limit)` pieces are spread over the whole
    configured span (2000-01-01..2100-01-01) and the one piece holding the data is
    then recursed down to day granularity — 1155 HTTP requests to load 210114
    order items, against 45 for the pager at the adjacent change to
    PAGE_OVERLAP_FRACTION. The two were measured together and land together.
    """
    if not single_limit:
        single_limit = _effective_limit(path, params)
    start_raw, end_raw = params[start_key], params[end_key]
    start_dt, date_only = _parse_window(start_raw)
    end_dt, _ = _parse_window(end_raw)
    url = f"{VERISIM_API_URL}{path}"

    stack = [(start_dt, end_dt, 0)]
    depth_cap = 60
    while stack:
        lo, hi, depth = stack.pop()
        sub = {**params, start_key: _fmt_window(lo, date_only),
               end_key: _fmt_window(hi, date_only)}
        total = _probe_window(path, sub)
        if total == 0:
            continue

        if total is None or total <= single_limit:
            # One request, offset=0: complete by construction.
            resp = _request_page(url, {**sub, "limit": single_limit, "offset": 0},
                                 120, path)
            resp.raise_for_status()
            data = resp.json()
            page = data if isinstance(data, list) else data.get("data", [])
            if page:
                log.info("%s: window %s..%s → %d rows (single request)",
                         path, sub[start_key], sub[end_key], len(page))
                yield page
            continue

        if lo >= hi or depth >= depth_cap or (hi - lo).total_seconds() < 1:
            log.warning(
                "%s: window %s..%s holds %d rows (> %d) and cannot be split "
                "further — falling back to offset paging (complete by "
                "construction since the source's ORDER BY became a total order, "
                "verisim t_d7892e10; the load reconciliation still fails the task "
                "if a row turns out to be unreachable)", path, sub[start_key],
                sub[end_key], total, single_limit,
            )
            yield from _fetch_pages(path, sub)
            continue

        pieces = max(2, -(-total // single_limit))  # ceil
        pieces = min(pieces, 4096)
        span = (hi - lo) / pieces
        edges = [lo + span * i for i in range(pieces + 1)]
        edges[0], edges[-1] = lo, hi
        for a, b in reversed(list(zip(edges, edges[1:]))):
            stack.append((a, b, depth + 1))


def _fetch_pages(path: str, params: dict, max_page_fetch: int | None = None):
    """Generator: yield one page of rows at a time, never accumulating all rows in memory.

    Consecutive pages deliberately **overlap** by 1/PAGE_OVERLAP_FRACTION of the
    page size, so a row sitting on a page boundary is requested twice (the
    primary-key upsert makes the repetition harmless). See "why the overlap is
    still here" below — it is a guard, not the completeness mechanism.

    Pagination stops when one of these conditions is met (checked in order):
    1. Empty page → source has no more data (return)
    2. Page has fewer rows than requested → last page (return)
    3. Page count exceeds MAX_PAGES → RuntimeError (infinite-loop guard)
    4. offset >= snapshot_total from the API's `total` field → boundary reached (return)

    The page size is the largest the route accepts (see _effective_limit), so a
    table that fits in one page is fetched whole and never crosses a boundary.

    Transport errors and 5xx responses are retried (see _request_page) and raise
    RuntimeError once retries are exhausted. If the API advertised a `total` and
    pagination ends with fewer rows than that snapshot, RuntimeError is raised —
    silently-dropped rows are a partial load posing as success.

    Why the overlap is still here, and why it is 1/20 and not 1/2 (t_4f3f46fd).

    The overlap was introduced at 1/2 by t_7c88f2f9, when the source views paged
    with `ORDER BY <non-unique column> LIMIT/OFFSET`: a tie cluster straddling a
    page boundary could be pushed into the previous page, so a row was never
    returned by any page while `total` still matched (measured then: 332 of 5986
    loyalty members, 273 of 7140 price-history rows, 5 of 2094 hr_schedules rows).
    It was never what made those loads complete — `pos_loyalty_members` holds 5647
    of its 5989 rows on one `points_balance` and its route caps pages at 5000, so
    no overlap could cover that cluster, and the load reconciliation was always
    the thing that caught the loss.

    The source fixed the ORDER BY on 2026-09-21 (verisim t_d7892e10: every
    paginated route's ORDER BY now ends on the table's primary key). The tie
    clusters themselves are unchanged — re-measured from inside this DAG, the
    widest are still 5647 (loyalty-members), 1907 (order-items), 495
    (price-history), 307 (order-events), 100 (return-items), 59 (schedules) — so
    the failure mode is still reachable if that ordering regresses. Against the
    fixed build, 23 consecutive-offset walks at limit=1000 with **no** overlap
    returned exactly `total` distinct primary keys on the first pass, on all six
    routes that used to lose rows, with identical primary-key sets on every repeat
    (order-events 59161, loyalty-members 5989, price-history 7420, return-items
    13490, schedules 2094, order-items 210114).

    Paging is therefore complete by construction and the reconciliation is the
    guarantee; this overlap is only a boundary trip-wire. It is kept small because
    a half-page overlap is a real cost once whole-table routes page (see
    ingest_table): measured back to back on the dev source, generator frozen,
    identical key sets in every arm —

        online/order-items   210114 rows  bisection: 1155 req  88.6 MB  17.2 s
                                        overlap 1/2:  84 req 117.2 MB  23.7 s
                                        overlap 1/20: 45 req  62.1 MB  12.8 s
        online/order-events   59161 rows  bisection:   87 req  11.9 MB   2.9 s
                                        overlap 1/2:  23 req  22.7 MB   3.3 s
                                        overlap 1/20: 13 req  12.4 MB   1.9 s
        pos/price-history      7445 rows  bisection:  105 req   2.7 MB   1.0 s
                                        overlap 1/2:  14 req   2.7 MB   0.5 s
                                        overlap 1/20:  8 req   1.5 MB   0.3 s

    Half a page doubled the bytes of every paged table (417614 rows fetched for
    210114 distinct on order-items) and, with the bisection gone, made whole-table
    loads slower than the bisection they replaced. 1/20 keeps the boundary row
    duplicated between two requests without paying for rows that are not distinct.
    """
    url = f"{VERISIM_API_URL}{path}"
    page_limit = max_page_fetch or _effective_limit(path, params)
    step = max(1, page_limit - page_limit // PAGE_OVERLAP_FRACTION)
    offset = 0
    snapshot_total = None
    pages = 0
    fetched = 0
    timeout_s = 120
    log.info("%s: paging with limit=%d, step=%d (pages overlap by %d rows)",
             path, page_limit, step, page_limit - step)

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

        offset += step

        # Snapshot boundary: we've consumed all rows that existed when we started
        if snapshot_total is not None and offset >= snapshot_total:
            log.info("  %s: reached snapshot boundary (offset=%d, total=%d)", path, offset, snapshot_total)
            return


def _coerce(val: Any) -> Any:
    if isinstance(val, (dict, list)):
        import json
        return json.dumps(val)
    return val


def _ingest_lock_key(raw_schema: str, raw_table: str) -> str:
    """Lock key for one raw table (`classid` is the module constant)."""
    return f"{raw_schema}.{raw_table}"


def _holder_of_table_lock(conn, key: str) -> str:
    """Best-effort "who holds this lock" for a failure message. Never raises.

    Runs on a fresh connection so a diagnostic can never abort the caller's
    transaction (a failed statement would leave its session unusable).

    Matches on the two lock keys only. The third column (`objsubid`) is not part
    of the key here: this cluster reports `objsubid = 2` even for the
    `pg_advisory_lock(int4, int4)` form, so filtering on the documented `1` finds
    nothing while the lock is held — which is exactly how this diagnostic first
    read "not in pg_locks" for a lock it was holding (t_657cebc3, test 3f).
    """
    try:
        probe = _edw_conn()
        try:
            with probe.cursor() as cur:
                cur.execute(
                    """
                    select coalesce(nullif(a.application_name, ''), '?')
                           || ' pid ' || l.pid
                    from pg_locks l
                    join pg_stat_activity a on a.pid = l.pid
                    where l.locktype = 'advisory'
                      and l.classid = %s::oid
                      and l.objid = hashtext(%s)::oid
                      and l.granted
                    """,
                    (INGEST_LOCK_NAMESPACE, key),
                )
                rows = [r[0] for r in cur.fetchall()]
        finally:
            probe.close()
    except Exception as exc:  # noqa: BLE001 — diagnostics must not mask the real error
        return f"holder unknown ({exc.__class__.__name__})"
    if not rows:
        return "holder not in pg_locks (lock taken by a session that has since gone)"
    return "held by " + ", ".join(rows)


def _acquire_table_lock(conn, raw_schema: str, raw_table: str, task_id: str) -> None:
    """Take the single-writer lock for one raw table, or fail loudly.

    Session-scoped on purpose: this task commits once per page, and a
    transaction-scoped lock would be released at the first commit — exactly when
    the table is half-loaded and most vulnerable. The lock dies with the
    connection, so a killed task or worker cannot leave the table locked.

    Waiting is bounded (`INGEST_LOCK_WAIT_S`): a loader that can never win must
    die saying which table and who holds it, not hang until the task's
    execution_timeout takes the whole run down with it.

    Raises RuntimeError when the wait expires.
    """
    key = _ingest_lock_key(raw_schema, raw_table)
    deadline = time.monotonic() + INGEST_LOCK_WAIT_S
    waited = False
    while True:
        with conn.cursor() as cur:
            cur.execute(
                "select pg_try_advisory_lock(%s, hashtext(%s))",
                (INGEST_LOCK_NAMESPACE, key),
            )
            acquired = cur.fetchone()[0]
        if acquired:
            if waited:
                log.info("[%s] acquired ingest lock for %s (had to wait)", task_id, key)
            return
        if not waited:
            waited = True
            log.warning(
                "[%s] another loader holds %s — waiting up to %ss (%s)",
                task_id, key, INGEST_LOCK_WAIT_S, _holder_of_table_lock(conn, key),
            )
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"refusing to load {key}: another ingest has held its writer lock for "
                f"more than {INGEST_LOCK_WAIT_S}s — {_holder_of_table_lock(conn, key)}. "
                "Two loaders on one raw table destroy each other; serialise the runs "
                "instead (AGENTS.md: 'one writer on the raw layer')."
            )
        time.sleep(INGEST_LOCK_POLL_S)


def _ensure_schema(conn, schema: str) -> None:
    """Create the schema if absent, serialised against concurrent creators.

    `CREATE SCHEMA IF NOT EXISTS` is NOT race-safe in PostgreSQL: the existence
    check runs before the namespace is locked, so two tasks that create the same
    schema at the same instant can both pass it and one then dies on
    pg_namespace_nspname_index. Every ingest task does this before it touches a
    table, and a schema like `raw_pos` has nine tasks under it, so at pool
    concurrency > 1 the race is the norm rather than an edge case. Two fresh
    loads on 2026-09-21 (t_7c88f2f9) each lost two tasks to it:

        duplicate key value violates unique constraint "pg_namespace_nspname_index"
        DETAIL:  Key (nspname)=(raw_inv) already exists.

    (inv_shrinkage_events + fulfillment_orders, then hr_locations +
    pricing_weekly_ads — all four green on their retry, i.e. a coin flip inside a
    run whose whole purpose is to be reproducible.)

    A transaction-scoped advisory lock per schema name makes the loser of the
    race wait for the winner and then find the schema already there. Both call
    sites go through here so the class stays fixed; callers commit.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (schema,))
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def _ensure_table(conn, schema: str, table: str, pk_col: str, sample_row: dict) -> None:
    """Create schema and table from a sample API row if they don't exist."""
    cols = [c for c in sample_row.keys() if not c.startswith("_sdc")]
    col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
    _ensure_schema(conn, schema)
    with conn.cursor() as cur:
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
    cut_from=(),
    **context,
) -> dict:
    params_conf = context.get("params") or {}
    now_iso = datetime.now(timezone.utc).isoformat()

    # The run's read cut. A referencing route reads before the route it
    # references (REFERENCING_ROUTES in the DAG body), so the referenced route's
    # window has to END where its referencing routes' reads ended — not at its own
    # task start, which is a scheduling gap later than them. Both invariants the
    # online pair is judged on need the two routes to share one instant:
    #   * an event that was read must find its parent order loaded → the parent's
    #     coverage has to REACH the child's read end;
    #   * a completed order that was loaded must find its final event loaded →
    #     the parent's coverage must NOT PASS the child's read end.
    # Read in that order and the gap between them is the whole defect; read in
    # this one and it is the last thing that is left of it. `cut_from` names the
    # tasks that reference this route; each returns the instant its own read
    # ended, and this route's window ends at the LATEST of them — a referenced
    # route reads at least as far as everything that references it, which is what
    # the foreign keys need (an event's parent is loaded because the parent read
    # past the events read; the earliest child's end would leave the events route
    # reading past the orders window, measured: 29 orphans on the dev slot). A
    # route nothing references keeps its own task start (the previous behaviour),
    # and a `full` route ignores the cut: it reads the whole table, so it already
    # covers any cut its children can report.
    #
    # An AS_OF_ROUTES route is stricter: it does not take the LATEST of its
    # children's cuts but the cut of the one that shares its clock
    # (`bounded_by`), because its window is anchored on a STATE clock and the
    # bound has to be the instant the child's window ended at, not a sibling's
    # later start. See AS_OF_ROUTES.
    as_of = AS_OF_ROUTES.get(task_id)
    instant = SNAPSHOT_BOUND_ROUTES.get(task_id)
    if as_of and as_of["bounded_by"] not in cut_from:
        # A wiring error, not a data condition: the route would be read as a
        # state snapshot with no snapshot boundary — the residual AS_OF_ROUTES
        # exists to close — and every later run would inherit whatever it loaded.
        raise RuntimeError(
            f"[{task_id}] is declared as-of ({as_of['state_clock']}) but "
            f"{as_of['bounded_by']} is not one of its cut providers "
            f"(cut_from={list(cut_from) or 'none'}), so its window end would fall "
            "back to this task's own start. Refusing to load: fix the "
            "AS_OF_ROUTES/REFERENCING_ROUTES wiring in the DAG body."
        )
    if instant and instant not in cut_from:
        # Same class of wiring error: this route has to end at another route's
        # read instant (the FK cluster is judged there), and its own task start
        # is a later instant the routes it must agree with cannot see.
        raise RuntimeError(
            f"[{task_id}] is declared snapshot-bound to {instant} but "
            f"{instant} is not one of its cut providers "
            f"(cut_from={list(cut_from) or 'none'}), so its window end would fall "
            "back to this task's own start. Refusing to load: fix the "
            "SNAPSHOT_BOUND_ROUTES wiring in the DAG body."
        )
    read_cut = None
    if cut_from:
        # `cut_from` names routes the way TABLE_CONFIGS and REFERENCING_ROUTES do,
        # but XCom is keyed on the rendered task_id — TaskGroup namespacing
        # prefixes it (`ingest_online.online_order_events`). Resolve against the
        # DAG's own tasks so a rename cannot silently degrade to the fallback.
        by_route = {t.task_id.rsplit(".", 1)[-1]: t.task_id
                    for t in context["dag"].tasks}
        unknown = sorted(set(cut_from) - set(by_route))
        if unknown:
            log.warning("[%s] cut_from names task(s) this DAG does not have: %s",
                        task_id, ", ".join(unknown))
        named = [(name, by_route[name]) for name in cut_from if name in by_route]
        pulled = context["ti"].xcom_pull(task_ids=[t for _, t in named]) if named else None
        if not isinstance(pulled, list):
            pulled = [pulled] if named else []
        cuts = {}
        for (name, _task_id), value in zip(named, pulled):
            raw_cut = value.get("cut") if isinstance(value, dict) else None
            if not raw_cut:
                continue
            try:
                parsed = datetime.fromisoformat(raw_cut)
            except (TypeError, ValueError):
                log.warning("[%s] unparseable read cut %r from a referencing task",
                            task_id, raw_cut)
                continue
            cuts[name] = (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc),
                          raw_cut)
        if as_of:
            # An as-of route's bound is not a choice between its children: it is
            # the window end of the one that shares its clock. Without it the load
            # would be a state read with no snapshot boundary — the residual
            # AS_OF_ROUTES exists to close — so it refuses rather than falling
            # back to its own task start.
            provider = as_of["bounded_by"]
            if provider not in cuts:
                raise RuntimeError(
                    f"[{task_id}] is declared as-of ({as_of['state_clock']}) but "
                    f"{provider} returned no read cut, so its window end would fall "
                    f"back to this task's own start and the state it loads need not "
                    f"match {provider}'s snapshot. Refusing to load: fix the "
                    "AS_OF_ROUTES/REFERENCING_ROUTES wiring in the DAG body."
                )
            read_cut = cuts[provider][1]
            log.info("[%s] as-of bound: %s takes the window end of %s (%s) — its own "
                     "task start is deliberately NOT used", task_id,
                     as_of["state_clock"], provider, read_cut)
        elif instant:
            # The same refusal, for the FK side of the cluster: this route ends
            # where the state route's instant is, so an insert-clock row it loads
            # cannot reference a parent the state-bounded window does not carry.
            if instant not in cuts:
                raise RuntimeError(
                    f"[{task_id}] is declared snapshot-bound to {instant} but "
                    f"{instant} returned no read cut, so its window end would fall "
                    f"back to this task's own start and it could load a row whose "
                    f"parent {instant} did not read. Refusing to load: fix the "
                    "SNAPSHOT_BOUND_ROUTES wiring in the DAG body."
                )
            read_cut = cuts[instant][1]
            log.info("[%s] snapshot bound: %s ends at %s's read instant (%s) — its "
                     "own task start is deliberately NOT used", task_id,
                     watermark_col, instant, read_cut)
        elif cuts:
            read_cut = max(cuts.values())[1]
        else:
            log.warning(
                "[%s] none of %s returned a read cut — this window ends at this "
                "task's own start instead, so a generator tick landing between "
                "their reads and this one can be loaded as a parent whose child "
                "row its referencing route did not read",
                task_id, ", ".join(cut_from),
            )

    conn = _edw_conn()
    try:
        # Single-writer lock first, and for the whole task: the DROP/TRUNCATE
        # below is exactly what two loaders of one table cannot both do. Taken
        # before _ensure_schema so the lock order is always table → schema.
        _acquire_table_lock(conn, raw_schema, raw_table, task_id)

        # Ensure schema exists before any table operation. Goes through
        # _ensure_schema so concurrent tasks for the same schema (raw_pos has
        # nine) cannot both pass the existence check and collide on
        # pg_namespace_nspname_index — see the note there.
        _ensure_schema(conn, raw_schema)
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

        # `end` is an incremental load's coverage end (its window end); a
        # whole-table load has none. Initialised here so the cut returned at the
        # end of the task is total whatever the config says.
        end = None

        if strategy == "full":
            if table_exists:
                with conn.cursor() as cur:
                    cur.execute(f'TRUNCATE "{raw_schema}"."{raw_table}"')
                conn.commit()
                log.info("[%s] truncated %s.%s for full reload", task_id, raw_schema, raw_table)
            # If the endpoint requires date params, pass a wide window covering all history
            if api_start_param:
                # End bound is deliberately far in the future, not "today": a full
                # load means every row the source holds, and history tables carry
                # future-dated rows — hr.schedules keeps a week of shifts ahead,
                # and a `today` bound silently dropped 631 of its 2094 rows
                # (t_7c88f2f9).
                fetch_params: dict = {api_start_param: "2000-01-01",
                                      api_end_param: WINDOW_FAR_FUTURE}
                log.info("[%s] full reload with date window 2000-01-01 → %s",
                         task_id, WINDOW_FAR_FUTURE)
            else:
                fetch_params = {}

        else:  # incremental
            if params_conf.get("start_dt") and params_conf.get("end_dt"):
                start = params_conf["start_dt"]
                end = params_conf["end_dt"]
                if as_of or instant:
                    # Even a param-driven reload keeps an as-of or snapshot-bound
                    # route's END at the instant the route it shares the cluster
                    # with read to. Capping the end cannot narrow the reload: that
                    # route is windowed (test_load_order.py 5d/6c) and a param run
                    # gives it the same `end_dt`, so its cut is that value. It is
                    # what keeps a wide-window run from loading a state newer than
                    # the events snapshot it was read against — or an item whose
                    # order that snapshot does not carry.
                    end = read_cut
                log.info("[%s] param window: %s → %s%s", task_id, start, end,
                         " (end held at the cluster instant)" if (as_of or instant) else "")
            elif table_exists and watermark_col in raw_cols:
                start = _get_watermark(conn, raw_schema, raw_table, watermark_col)
                end = read_cut or now_iso
                lookback_days = INCREMENTAL_LOOKBACK_DAYS.get(task_id)
                if lookback_days:
                    # The watermark is a backdating column: the rows that were
                    # inserted since the last run may sit *below* it, so the
                    # window has to reach back past it (see the constant's
                    # comment). Re-upserting the trailing window is deliberate —
                    # the PK upsert makes it idempotent, and it is what heals a
                    # partial load left by an earlier run. No entry is configured
                    # today; the branch is the retained implementation of the
                    # shape (see the constant).
                    wm_dt, date_only = _parse_window(start)
                    start = _fmt_window(wm_dt - timedelta(days=lookback_days),
                                        date_only)
                    log.info("[%s] watermark on %s is a backdating column — "
                             "reaching %d days back of it: window %s → %s",
                             task_id, watermark_col, lookback_days, start, end)
                else:
                    log.info("[%s] watermark window: %s → %s%s", task_id, start, end,
                             f" [read cut from {', '.join(cut_from)}]" if read_cut else "")
            elif table_exists:
                # Populated table, no usable watermark: it does not hold the
                # column this config watermarks on. That is a transition, not a
                # steady state — it happens on the first run after a table
                # adopts a new watermark column, which reaches the raw table
                # only with the load that carries it (added by
                # `_detect_schema_drift` as TEXT during the fetch).
                # It has fired twice: `created_at` on return_items (t_b474c79e)
                # and on the online pair (t_886f7d67 — the POS pair never
                # reached it, because the gap heal that preceded their switch
                # was itself a load of the new payload and delivered the column).
                # There is no delta to compute, so the load takes the same
                # bounded fallback a brand-new table takes, and the column is in
                # place for the next run.
                #
                # Bounded is the word that matters: a row older than the fallback
                # horizon is outside this window and every later one, so this
                # branch is only complete when the table's whole history sits
                # inside the horizon (true for the returns tables — every row of
                # both is an insert of the current day — and for the four
                # backdated tables: their sources are minutes-to-hours old
                # datasets whose earliest insert is the seed of the same day).
                # Otherwise the adoption owes a param-driven full reload
                # (airflow/README.md → "Forcing a full reload") or a one-off
                # `full` run: nothing here would notice the shortfall, because
                # the run reconciliation only knows the *requested window's*
                # advertised total and verify_raw_vs_source fails on excess only.
                # Verify an adoption against the source key-for-key rather than
                # by row count.
                fb = datetime.now(timezone.utc) - timedelta(days=INCREMENTAL_FALLBACK_DAYS)
                start = fb.isoformat()
                end = read_cut or now_iso
                log.warning(
                    "[%s] %s.%s has rows but no %s column to watermark on — "
                    "falling back to the last %d days (%s → %s). Only a "
                    "transition (the adoption of a new watermark column) reaches "
                    "this branch; check that the table's history is inside the "
                    "horizon, and full-reload it if not",
                    task_id, raw_schema, raw_table, watermark_col,
                    INCREMENTAL_FALLBACK_DAYS, start, end,
                )
            else:
                fb = datetime.now(timezone.utc) - timedelta(days=INCREMENTAL_FALLBACK_DAYS)
                start = fb.isoformat()
                end = read_cut or now_iso
                log.info("[%s] no table yet — fallback window: %s → %s", task_id, start, end)

            # Some endpoints expect date-only (YYYY-MM-DD) not full ISO timestamps
            fetch_params = {
                api_start_param: _fmt_param(start, api_start_param),
                api_end_param:   _fmt_param(end,   api_end_param),
            }

        # Fetch mode. Where a window is available, fetch window-at-a-time
        # (_fetch_all): paging *inside* a window is not safe against the source
        # views' non-unique ORDER BY, and the loss is invisible because the API's
        # own row count still matches. A route whose config names no window is
        # checked against the API's OpenAPI document (_discover_window) — several
        # accept one the config does not use. What is left can only be paged:
        # those pages overlap by design (_fetch_pages) and the reconciliation
        # below still fails the task if a row cannot be reached at all.
        #
        # That second sentence is history as of t_4f3f46fd. Discovery existed to
        # keep *whole-table* loads complete while the source's paging was lossy;
        # the source fixed its ORDER BY (verisim t_d7892e10), and the premise was
        # re-measured on the fixed build from inside this DAG: 23 consecutive-
        # offset walks at limit=1000 with no overlap returned exactly `total`
        # distinct primary keys on every pass, on all six routes that used to lose
        # rows. So a `full` route now pages and no longer bisects a window it did
        # not declare. A route whose *config* declares a window keeps the windowed
        # load: there the window is the load's own semantics (a watermark delta),
        # not a way around a page boundary.
        #
        # This gating and PAGE_OVERLAP_FRACTION were decided together and have to
        # land together: handing whole tables to the pager while it still paid a
        # half-page overlap was *worse* than the bisection it replaced (measured,
        # generator frozen, same 210114 distinct keys every arm — order-items
        # bisection 1155 req/88.6 MB/17.2 s, paging@1/2 84 req/117.2 MB/23.7 s,
        # paging@1/20 45 req/62.1 MB/12.8 s; order-events 87/11.9 MB/2.9 s vs
        # 13/12.4 MB/1.9 s). Only the pair beats both predecessors on requests,
        # bytes and time.
        window_keys = (api_start_param, api_end_param) if (
            api_start_param and api_end_param) else None
        if window_keys:
            # Only trust the configured window if the route really declares those
            # parameters. FastAPI silently ignores unknown query parameters, so a
            # config that names a window the route does not accept looks like a
            # windowed fetch while returning the whole table for every "window" —
            # `online_order_items` did exactly that and spent its run splitting a
            # year into 43 pieces that each reported all 210114 rows.
            route_params = _route_query_params(api_path)
            if route_params is not None and not set(window_keys) <= route_params:
                log.warning(
                    "[%s] config declares %s/%s but %s does not accept them "
                    "(route params: %s) — paging with overlap instead of paging "
                    "inside a window that does not filter",
                    task_id, api_start_param, api_end_param, api_path,
                    ", ".join(sorted(route_params)) or "none")
                window_keys = None
        if window_keys is None and strategy != "full":
            window_keys = _discover_window(api_path)
            if window_keys:
                start_key, end_key = window_keys
                lo_dt = datetime.now(timezone.utc) - timedelta(
                    days=INCREMENTAL_FALLBACK_DAYS)
                lo = ("2000-01-01" if strategy == "full" else lo_dt.isoformat())
                hi = WINDOW_FAR_FUTURE if strategy == "full" else now_iso
                fetch_params = {
                    start_key: _fmt_param(lo, start_key),
                    end_key:   _fmt_param(hi, end_key),
                }
                log.info("[%s] route accepts %s/%s — fetching %s → %s instead of paging",
                         task_id, start_key, end_key,
                         fetch_params[start_key], fetch_params[end_key])
        if window_keys is None and strategy == "full":
            declared_window = _discover_window(api_path)
            if declared_window:
                log.info(
                    "[%s] %s accepts %s/%s but this is a whole-table load — paging "
                    "instead of bisecting a window (t_4f3f46fd: the source's ORDER "
                    "BY is a total order since verisim t_d7892e10, so paging is "
                    "complete by construction and costs 45 requests where the "
                    "bisection cost 1155)",
                    task_id, api_path, declared_window[0], declared_window[1])
        limit = _effective_limit(api_path, fetch_params)
        expected = _probe_window(api_path, fetch_params)
        if expected is not None:
            log.info("[%s] source reports %d rows for the requested window",
                     task_id, expected)
        page_iter = (
            _fetch_all(api_path, fetch_params, *window_keys, single_limit=limit)
            if window_keys
            else _fetch_pages(api_path, fetch_params, max_page_fetch=limit)
        )

        # Stream page-by-page into Postgres — never hold the full dataset in memory.
        # The PK set is kept only to verify the load landed what the source
        # advertised (distinct keys, so repeated rows from an unstable page order
        # cannot inflate the count); above the cap we skip the check and say so.
        written = 0
        page_num = 0
        seen_pks: set | None = set()
        for page in page_iter:
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
            if seen_pks is not None:
                seen_pks.update(str(r.get(pk_col)) for r in page)
                if len(seen_pks) > DISTINCT_TRACK_MAX:
                    log.warning(
                        "[%s] more than %d distinct keys — stopping the landed-row "
                        "reconciliation for this table", task_id, DISTINCT_TRACK_MAX)
                    seen_pks = None
            n = _upsert_rows(conn, raw_schema, raw_table, page, pk_col, raw_cols, now_iso)
            written += n
            page_num += 1
            log.info("[%s] page %d: inserted %d rows (total so far: %d)", task_id, page_num, n, written)

        # Every row-bearing read is behind us — the reconciliation probes below
        # count rows, they do not load them — so this is the instant up to which
        # this load provably saw the source. That is the cut this route hands to
        # the routes it references (see `cut_from`).
        read_end = datetime.now(timezone.utc)

        # Reconciliation: every row the source advertised for this window has to be
        # in the table. Measured against distinct keys rather than fetched rows,
        # because an unstable page order returns duplicates in place of the rows it
        # skipped — that is exactly how 271 rows of online.order_events went missing
        # on 2026-09-21 while every count still agreed. Re-probing afterwards keeps a
        # mid-run source rewrite (the generator does replace windows) from looking
        # like loss: the bar is the lower of the two probes.
        if expected is not None and seen_pks is not None:
            expected_after = _probe_window(api_path, fetch_params)
            bar = expected if expected_after is None else min(expected, expected_after)
            landed = len(seen_pks)
            if landed < bar and task_id in SOURCE_PARTIAL:
                # `active_only` endpoints serve a subset of their relation by
                # design, so a shortfall here is expected (see SOURCE_PARTIAL).
                log.info("[%s] subset endpoint — %d distinct rows landed vs %d "
                         "advertised; shortfall expected, not a failure",
                         task_id, landed, bar)
            elif landed < bar:
                raise RuntimeError(
                    f"[{task_id}] {api_path}: source advertised {bar} rows for the "
                    f"requested window (probes {expected}/{expected_after}) but only "
                    f"{landed} distinct rows could be loaded — {bar - landed} row(s) "
                    f"are unreachable through the API's paging. Refusing to report a "
                    f"partial load as success (see _fetch_all)."
                )
            else:
                log.info("[%s] reconciliation ok — %d distinct rows landed vs %d advertised",
                         task_id, landed, bar)

        log.info("[%s] done — %d total rows written to %s.%s", task_id, written, raw_schema, raw_table)

        # The cut this route offers the routes it references. An incremental
        # load's coverage ends at its window end (`end`): rows newer than that
        # are filtered out of the load even though they were physically read. A
        # whole-table load has no window, so its coverage ends where its reads
        # did — and everything it read, it read before `read_end`.
        return {
            "task_id": task_id,
            "rows": written,
            "cut": (end or read_end.isoformat()) if strategy == "incremental"
                   else read_end.isoformat(),
        }

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
    # LOAD-BEARING — do not remove (t_657cebc3). The ingest truncates before it
    # reloads, so two runs of this DAG destroy each other. This line guards one
    # scheduler; the per-table advisory lock in ingest_table guards the data
    # (a second Airflow, a hand-run loader, the raw-layer recovery recipe).
    max_active_runs=1,
    max_active_tasks=4,
    params={"start_dt": None, "end_dt": None},
    tags=["grocery", "api", "ingest", "granular"],
) as dag:

    # The run's read cut: each route that something references takes the earliest
    # read end of its referencing routes as the end of its own window (see
    # `cut_from` in ingest_table), so the two sides of a foreign key are judged at
    # one instant rather than across a scheduling gap.
    cut_from: dict = {}
    for child_route, parent_route in REFERENCING_ROUTES:
        cut_from.setdefault(parent_route, set()).add(child_route)
    # A snapshot-bound route ends at another route's instant on an edge that is
    # not a foreign key (SNAPSHOT_BOUND_ROUTES): same mechanism, so the route it
    # depends on is read first and the bound is one run's instant rather than a
    # sibling's own start.
    for route, instant_route in SNAPSHOT_BOUND_ROUTES.items():
        cut_from.setdefault(route, set()).add(instant_route)

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
                        "cut_from": sorted(cut_from.get(tid, ())),
                    },
                    execution_timeout=timedelta(minutes=60),
                ))

    # Read order (see REFERENCING_ROUTES): a referencing route finishes reading
    # before the route it references starts, so the referenced route's window
    # covers every parent row its children could have picked up. Airflow
    # dependencies are transitive here, so a chain like
    # `pos_return_items → pos_transaction_items → pos_transactions` is ordered
    # by declaring the two adjacent edges only.
    # TaskGroup namespacing prefixes the task_id (`ingest_pos.pos_transactions`),
    # so key the lookup on the last component — the route name — and assert the
    # mapping is 1:1 so a name collision cannot silently drop an edge.
    by_task_id = {t.task_id.rsplit(".", 1)[-1]: t for t in ingest_tasks}
    assert len(by_task_id) == len(ingest_tasks), "route names collide after TaskGroup prefixes"
    for child_route, parent_route in REFERENCING_ROUTES:
        by_task_id[child_route] >> by_task_id[parent_route]
    # ... and the snapshot-bound route is read after the route whose instant it
    # ends at (SNAPSHOT_BOUND_ROUTES), which is what makes its cut available.
    for route, instant_route in SNAPSHOT_BOUND_ROUTES.items():
        by_task_id[instant_route] >> by_task_id[route]

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
