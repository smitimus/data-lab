#!/usr/bin/env python3
"""
Incremental watermark tests — the insert clock on the backdated tables
=====================================================================
Run INSIDE the airflow-worker container (needs airflow importable, and reaches
both the source API and the source DB over the shared network for the live
checks):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_incremental_watermarks.py

No pytest required — plain asserts, exit code 0 = pass.

Supersedes test_incremental_lookback.py (t_4788529f), which pinned the bounded
21-day reach-back `pos_returns` / `pos_return_items` used while their only window
was the backdated `return_dt`.

What it proves, and why each part is load-bearing:

  1. Configuration. The six tables whose date columns the source's generator
     backdates watermark on the INSERT clock (`created_at`, `DEFAULT NOW()`
     written by the same statement as the row) and window it with
     `created_after`/`created_before` (verisim `373cbe3` for the returns,
     `4ec85ec` for the POS and online pairs). A refactor that puts any of them
     back on `return_dt` / `transaction_dt` / `placed_dt` silently reintroduces
     the defect this replaced: those columns are stamped with the simulated time
     a batch belongs to, not the moment it was written, so a window anchored at
     their MAX() cannot see the backdated half of a batch. Measured on the dev
     slot: 53 of the source's 80 returns in the 2026-09-21T07:08:27Z batch, and
     10 transactions / 129 lines of a POS gap-fill that sat below raw's own
     MAX(transaction_dt) and was the source of a live dbt WARN.

  2. No configured table carries a bounded reach-back (`INCREMENTAL_LOOKBACK_DAYS`
     is empty). Any entry is a deliberate, documented heal for a backdating
     watermark; re-adding one for a table that has an insert clock would silently
     re-widen a window that is now exact.

  3. Behaviour, driven through `ingest_table` with the DB and HTTP seams stubbed
     (no EDW, no source, nothing written):
       - each table fetches `[watermark, now]` on its own configured bound names,
         with the start bound passed through untouched (not shifted);
       - a populated table whose watermark column has not reached the raw table
         yet (the first load after an adoption) falls back to the bounded horizon
         *and says so at WARNING* naming the column, instead of raising
         UndefinedColumn or loading a narrower window;
       - an explicit DAG-param window (`{"start_dt", "end_dt"}`) is passed
         through onto `created_after`/`created_before` — the documented full
         reload, and the one caveat the config comments carry;
       - the reach-back mechanism still shifts a window when a table IS
         registered in `INCREMENTAL_LOOKBACK_DAYS`, so the retained branch is
         proven working rather than dead code.

  4. Live contract (needs the source). Every one of the six routes really
     declares `created_after`/`created_before` **and** really filters on them:
     FastAPI silently ignores unknown query parameters, so a config naming a
     bound the route does not implement looks like a windowed fetch while
     returning the whole table — the failure mode that cost `online_order_items`
     a run of 43 fake windows. Filtering is checked against the source DB, not
     against the spec: for a CLOSED window (`created_before` in the past) the
     route's advertised `total` must equal the source's own SQL count of that
     window, which a route that ignored the bounds cannot match.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

DAGS_DIR = os.environ.get("DAGS_DIR", "/opt/airflow/dags")
sys.path.insert(0, DAGS_DIR)

SOURCE_URL = os.environ.get("VERISIM_API_URL", "http://verisim-grocery:8000")

# task_id -> the route the live checks hit, and the source SQL the window is
# counted with. The header relation's `created_at` is the clock for all six;
# the two item routes join it (neither table has a timestamp of its own).
LIVE_ROUTES = {
    "pos_transactions": (
        "/grocery/pos/transactions",
        "select max(created_at) from pos.transactions",
        "select count(*) from pos.transactions"
        " where created_at >= %(a)s::timestamptz and created_at <= %(b)s::timestamptz",
        "pos.transactions",
    ),
    "pos_transaction_items": (
        "/grocery/pos/transaction-items",
        "select max(created_at) from pos.transactions",
        "select count(*) from pos.transaction_items i"
        " join pos.transactions t using (transaction_id)"
        " where t.created_at >= %(a)s::timestamptz and t.created_at <= %(b)s::timestamptz",
        "pos.transaction_items",
    ),
    "pos_returns": (
        "/grocery/pos/returns",
        "select max(created_at) from pos.returns",
        "select count(*) from pos.returns"
        " where created_at >= %(a)s::timestamptz and created_at <= %(b)s::timestamptz",
        "pos.returns",
    ),
    "pos_return_items": (
        "/grocery/pos/return-items",
        "select max(created_at) from pos.returns",
        "select count(*) from pos.return_items i join pos.returns r using (return_id)"
        " where r.created_at >= %(a)s::timestamptz and r.created_at <= %(b)s::timestamptz",
        "pos.return_items",
    ),
    "online_orders": (
        "/grocery/online/orders",
        "select max(created_at) from online.orders",
        "select count(*) from online.orders"
        " where created_at >= %(a)s::timestamptz and created_at <= %(b)s::timestamptz",
        "online.orders",
    ),
    "online_order_items": (
        "/grocery/online/order-items",
        "select max(created_at) from online.orders",
        "select count(*) from online.order_items i join online.orders o using (order_id)"
        " where o.created_at >= %(a)s::timestamptz and o.created_at <= %(b)s::timestamptz",
        "online.order_items",
    ),
}

# The insert-clock shape every one of the six must be configured with.
WANT = ("incremental", "created_at", "created_after", "created_before")

# The backdating date columns that must NOT drive a watermark on these tables.
BACKDATING = {"return_dt", "transaction_dt", "placed_dt"}

PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS " if cond else "FAIL ") + name + (f" — {detail}" if detail else ""))


def load(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *a, **k):
        return None

    def fetchone(self):
        return (None,)

    def fetchall(self):
        return []


class _FakeConn:
    """Enough connection for the incremental path: no DDL, no reads."""

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        pass

    def close(self):
        pass


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


# The bounds the routes declare in /openapi.json (checked live as check 4a).
ROUTE_PARAMS = {"start_dt", "end_dt", "created_after", "created_before"}


def drive(mod, task_id, watermark, params=None, raw_cols=None, log_capture=None):
    """Run ingest_table's window computation for one configured entry, DB + HTTP stubbed.

    The entry is looked up in TABLE_CONFIGS and driven with its own strategy,
    watermark column and bound names, so the test exercises the configuration
    that ships rather than a hand-built tuple. `raw_cols` overrides the raw
    table's column list — that is what decides between the watermark branch and
    the transition fallback.

    Returns the `fetch_params` the loader would have sent to the API.
    """
    captured = {}
    saved = {name: getattr(mod, name) for name in (
        "_edw_conn", "_acquire_table_lock", "_ensure_schema",
        "_raw_columns", "_get_watermark", "_route_query_params",
        "_effective_limit", "_probe_window", "_fetch_all", "_fetch_pages",
    )}

    def fake_fetch_all(path, params, start_key, end_key, single_limit=None):
        captured["fetch_params"] = dict(params)
        return iter(())

    def fake_fetch_pages(path, params, max_page_fetch=None):
        captured["fetch_params"] = dict(params)
        return iter(())

    cfg = next(c for c in mod.TABLE_CONFIGS if c[0] == task_id)
    (_tid, api_path, raw_schema, raw_table, pk_col,
     strategy, watermark_col, api_start_param, api_end_param) = cfg

    mod._edw_conn = lambda: _FakeConn()
    mod._acquire_table_lock = lambda *a, **k: None
    mod._ensure_schema = lambda *a, **k: None
    # A populated table: the column list is what decides whether the watermark
    # branch (below) or the transition fallback is taken.
    cols = raw_cols if raw_cols is not None else [pk_col, watermark_col]
    mod._raw_columns = lambda *a, **k: list(cols)
    mod._get_watermark = lambda *a, **k: watermark
    mod._route_query_params = lambda path: set(ROUTE_PARAMS)
    mod._effective_limit = lambda *a, **k: 1000
    mod._probe_window = lambda *a, **k: None
    mod._fetch_all = fake_fetch_all
    mod._fetch_pages = fake_fetch_pages
    if log_capture is not None:
        logging.getLogger("gia_watermarks").addHandler(log_capture)
    try:
        mod.ingest_table(
            task_id=task_id, api_path=api_path,
            raw_schema=raw_schema, raw_table=raw_table, pk_col=pk_col,
            strategy=strategy, watermark_col=watermark_col,
            api_start_param=api_start_param, api_end_param=api_end_param,
            params=params or {},
        )
    finally:
        if log_capture is not None:
            logging.getLogger("gia_watermarks").removeHandler(log_capture)
        for name, fn in saved.items():
            setattr(mod, name, fn)
    return captured.get("fetch_params", {})


def source_conn(mod):
    import psycopg2
    conn = psycopg2.connect(**mod.VERISIM_DB)
    conn.set_session(readonly=True)
    return conn


def api_total(path, params):
    url = f"{SOURCE_URL}{path}?{urllib.parse.urlencode({**params, 'limit': 1, 'offset': 0})}"
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.load(r).get("total")


def main():
    gia = load(f"{DAGS_DIR}/grocery_ingest_api.py", "gia_watermarks")

    configs = {c[0]: c for c in gia.TABLE_CONFIGS}
    lookback = gia.INCREMENTAL_LOOKBACK_DAYS

    # ------------------------------------------------------------------
    # 1. configuration — six tables on the insert clock
    # ------------------------------------------------------------------
    for task_id in LIVE_ROUTES:
        cfg = configs.get(task_id)
        check(f"1a {task_id} is configured", cfg is not None)
        if cfg is None:
            continue
        got = (cfg[5], cfg[6], cfg[7], cfg[8])
        check(f"1b {task_id} is incremental on the insert clock "
              f"(strategy, watermark, bounds) == {WANT}", got == WANT, f"got={got}")
        check(f"1c {task_id} no longer watermarks on a backdating date column",
              cfg[6] not in BACKDATING and cfg[7] not in ("start_dt", "end_dt"),
              f"watermark={cfg[6]} bounds={cfg[7]}/{cfg[8]}")

    check("1d every registry entry carries a 9-field config",
          all(len(c) == 9 for c in gia.TABLE_CONFIGS))
    check("1e no table is configured `incremental` without a watermark column",
          all(c[6] for c in gia.TABLE_CONFIGS if c[5] == "incremental"))
    check("1f created_at is not a backdating watermark — no entry reaches back "
          "of it", not [t for t, d in lookback.items()
                        if configs.get(t, (None,) * 7)[6] == "created_at"],
          str({t: d for t, d in lookback.items()
               if configs.get(t, (None,) * 7)[6] == "created_at"}))

    # ------------------------------------------------------------------
    # 2. the bounded reach-back mechanism is retained, and unused
    # ------------------------------------------------------------------
    check("2a no configured table carries a lookback", lookback == {}, str(lookback))

    wm = "2026-09-21T00:59:45.810233-04:00"

    # Retained capability: registered under a table that has no insert clock (so
    # the registration is not a contradiction of check 1f), the branch still
    # shifts the window. Pinned so it cannot rot into dead code unnoticed.
    stand_in = next(t for t, c in configs.items()
                    if c[5] == "incremental" and c[6] not in ("created_at", *BACKDATING))
    saved_lookback = gia.INCREMENTAL_LOOKBACK_DAYS
    gia.INCREMENTAL_LOOKBACK_DAYS = {stand_in: 7}
    got = drive(gia, stand_in, wm)
    want_start = (datetime.fromisoformat(wm) - timedelta(days=7)).isoformat()
    check(f"2b a registered lookback still fetches [watermark - 7d, now] "
          f"(on {stand_in})",
          got.get(configs[stand_in][7]) == want_start,
          f"{configs[stand_in][7]}={got.get(configs[stand_in][7])} want={want_start}")
    gia.INCREMENTAL_LOOKBACK_DAYS = saved_lookback

    # ------------------------------------------------------------------
    # 3. behaviour — the window the loader actually requests
    # ------------------------------------------------------------------
    for task_id in LIVE_ROUTES:
        start_param, end_param = configs[task_id][7], configs[task_id][8]
        got = drive(gia, task_id, wm)
        check(f"3a {task_id} fetches [watermark, now] on its configured bounds, "
              f"unshifted",
              got.get(start_param) == wm and end_param in got
              and "start_dt" not in got and "end_dt" not in got,
              str(got))
        end = got.get(end_param, "")
        check(f"3b {task_id} keeps the end bound at now",
              end and datetime.fromisoformat(end) > datetime.fromisoformat(wm),
              f"{end_param}={end}")

    # A populated table whose watermark column has not reached the raw table yet
    # (the first load after an adoption) cannot watermark: it must fall back —
    # visibly — rather than raise UndefinedColumn or load a narrower window.
    # It fired for `pos_return_items` (t_b474c79e) and for the online pair
    # (t_886f7d67); the POS pair was healed before its switch, so it never did.
    for task_id in LIVE_ROUTES:
        cfg = configs[task_id]
        cap = _Capture()
        got = drive(gia, task_id, wm, raw_cols=[cfg[4], "some_date_col"],
                    log_capture=cap)
        start_param = cfg[7]
        start = got.get(start_param, "")
        lo = datetime.now(timezone.utc) - timedelta(days=gia.INCREMENTAL_FALLBACK_DAYS + 1)
        hi = datetime.now(timezone.utc) - timedelta(days=gia.INCREMENTAL_FALLBACK_DAYS - 1)
        check(f"3c {task_id}: a raw table without created_at falls back to the "
              f"bounded horizon instead of raising",
              start and lo < datetime.fromisoformat(start) < hi,
              f"{start_param}={start}")
        warned = [r.getMessage() for r in cap.records if r.levelno >= logging.WARNING]
        check(f"3d {task_id}: the fallback is announced at WARNING with the "
              f"column named",
              any("created_at" in m and "watermark" in m for m in warned),
              " | ".join(warned)[:200] or "no warning logged")

    # Explicit DAG params still win — the documented full-reload recipe. They
    # now land on the created_at bounds (the caveat the config comments carry):
    # harmless for 2000->2100, but it is not a date-column window any more.
    params = {"start_dt": "2000-01-01T00:00:00", "end_dt": "2100-01-01T00:00:00"}
    for task_id in LIVE_ROUTES:
        got = drive(gia, task_id, wm, params=params)
        check(f"3e {task_id}: an explicit param window is passed through onto "
              f"its configured bounds",
              got.get(configs[task_id][7]) == params["start_dt"]
              and got.get(configs[task_id][8]) == params["end_dt"], str(got))

    # ------------------------------------------------------------------
    # 4. live contract — the routes really declare the bounds, and really filter
    # ------------------------------------------------------------------
    spec = None
    spec_err = "openapi.json not read"
    try:
        with urllib.request.urlopen(f"{SOURCE_URL}/openapi.json", timeout=15) as r:
            spec = json.load(r)
    except Exception as exc:  # noqa: BLE001 — report, don't crash the suite
        spec_err = f"{type(exc).__name__}: {exc}"

    conn = None
    db_err = "source DB not read"
    try:
        conn = source_conn(gia)
    except Exception as exc:  # noqa: BLE001
        db_err = f"{type(exc).__name__}: {exc}"

    for task_id, (path, mx_sql, cnt_sql, rel) in LIVE_ROUTES.items():
        if spec is None:
            check(f"4a {path} declares created_after/created_before", False, spec_err)
            check(f"4b {path} still declares start_dt/end_dt", False, spec_err)
            continue
        names = None
        for spec_path, methods in spec.get("paths", {}).items():
            a = spec_path.strip("/").split("/")
            b = path.strip("/").split("/")
            if len(a) == len(b) and all(x == y or (x.startswith("{") and x.endswith("}"))
                                        for x, y in zip(a, b)):
                names = set()
                for method in methods.values():
                    if isinstance(method, dict):
                        names |= {p.get("name") for p in method.get("parameters", [])
                                  if p.get("in") == "query"}
        check(f"4a {path} declares created_after/created_before",
              names is not None and {"created_after", "created_before"} <= names,
              str(sorted(names)) if names else "route not in the spec")
        check(f"4b {path} still declares start_dt/end_dt (the date-column window)",
              names is not None and {"start_dt", "end_dt"} <= names,
              str(sorted(names)) if names else "route not in the spec")

        if conn is None:
            check(f"4c {path} really filters on created_after/created_before", False, db_err)
            continue
        # A CLOSED window, so the expected count cannot move under the check:
        # `total` for it must equal the source's own SQL count. A route that
        # ignored the bounds would answer with the whole table's count instead.
        try:
            with conn.cursor() as cur:
                cur.execute(mx_sql)
                mx = cur.fetchone()[0]
                cur.execute(cnt_sql, {"a": mx, "b": mx})
                sql_tie = cur.fetchone()[0]
                cur.execute(cnt_sql, {"a": mx - timedelta(minutes=30), "b": mx})
                sql_30 = cur.fetchone()[0]
                cur.execute(cnt_sql, {"a": mx - timedelta(minutes=30),
                                      "b": mx - timedelta(minutes=15)})
                sql_old = cur.fetchone()[0]
                cur.execute(f"select count(*) from {rel}")
                whole = cur.fetchone()[0]
            api_30 = api_total(path, {"created_after": (mx - timedelta(minutes=30)).isoformat(),
                                      "created_before": mx.isoformat()})
            api_old = api_total(path, {"created_after": (mx - timedelta(minutes=30)).isoformat(),
                                       "created_before": (mx - timedelta(minutes=15)).isoformat()})
            check(f"4c {path} really filters on created_after/created_before "
                  f"([max-30m, max]: api={api_30} sql={sql_30}; "
                  f"[max-30m, max-15m]: api={api_old} sql={sql_old}; "
                  f"whole table={whole}, tie cluster={sql_tie})",
                  api_30 == sql_30 and api_old == sql_old and api_30 > 0)
        except Exception as exc:  # noqa: BLE001
            check(f"4c {path} really filters on created_after/created_before", False,
                  f"{type(exc).__name__}: {exc}")

    if conn is not None:
        conn.close()

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED: " + ", ".join(FAIL))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
