#!/usr/bin/env python3
"""
Incremental watermark tests — the insert clock on the backdated tables, and the
state clock on the mutating one
=================================================================================
Run INSIDE the airflow-worker container (needs airflow importable, and reaches
both the source API and the source DB over the shared network for the live
checks):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_incremental_watermarks.py

No pytest required — plain asserts, exit code 0 = pass.

Supersedes test_incremental_lookback.py (t_4788529f), which pinned the bounded
21-day reach-back `pos_returns` / `pos_return_items` used while their only window
was the backdated `return_dt`.

What it proves, and why each part is load-bearing:

  1. Configuration. Six tables used to watermark on a date column the generator
     backdates. Five of them now watermark on the INSERT clock (`created_at`,
     `DEFAULT NOW()` written by the same statement as the row) and window it with
     `created_after`/`created_before` (verisim `373cbe3` for the returns,
     `4ec85ec` for the POS pair). A refactor that puts any of them back on
     `return_dt` / `transaction_dt` / `placed_dt` silently reintroduces the defect
     this replaced: those columns are stamped with the simulated time a batch
     belongs to, not the moment it was written, so a window anchored at their
     MAX() cannot see the backdated half of a batch. Measured on the dev slot: 53
     of the source's 80 returns in the 2026-09-21T07:08:27Z batch, and 10
     transactions / 129 lines of a POS gap-fill that sat below raw's own
     MAX(transaction_dt) and was the source of a live dbt WARN.

     The sixth, `online_orders`, is on the STATE clock (`updated_at`,
     `updated_after`/`updated_before`) since t_5a16129f, because its status moves
     long after the insert: an insert clock cannot see a state change on a row
     the watermark has passed, and it cannot be bounded by the child's read
     instant without losing the row instead of delaying it. Same shape of check
     (`clock_sets()`), different clock — see section 5 for what is specific to it.

  2. No configured table carries a bounded reach-back (`INCREMENTAL_LOOKBACK_DAYS`
     is empty). Any entry is a deliberate, documented heal for a backdating
     watermark; re-adding one for a table that has an insert clock would silently
     re-widen a window that is now exact.

  3. Behaviour, driven through `ingest_table` with the DB and HTTP seams stubbed
     (no EDW, no source, nothing written):
       - each table fetches `[watermark, bound]` on its own configured bound names,
         with the start bound passed through untouched (not shifted);
       - a populated table whose watermark column has not reached the raw table
         yet (the first load after an adoption) falls back to the bounded horizon
         *and says so at WARNING* naming the column, instead of raising
         UndefinedColumn or loading a narrower window;
       - an explicit DAG-param window (`{"start_dt", "end_dt"}`) is passed
         through onto the configured bounds — the documented full reload, and the
         one caveat the config comments carry — except that an as-of route holds
         its END at the child's instant (section 5f);
       - the reach-back mechanism still shifts a window when a table IS
         registered in `INCREMENTAL_LOOKBACK_DAYS`, so the retained branch is
         proven working rather than dead code.

  4. Live contract (needs the source). Every route really declares the bounds its
     config uses **and** really filters on them: FastAPI silently ignores unknown
     query parameters, so a config naming a bound the route does not implement
     looks like a windowed fetch while returning the whole table — the failure
     mode that cost `online_order_items` a run of 43 fake windows. Filtering is
     checked against the source DB, not against the spec: for a CLOSED window
     (`*_before` in the past) the route's advertised `total` must equal the
     source's own SQL count of that window, which a route that ignored the bounds
     cannot match.

  5. As-of behaviour (t_5a16129f). A route whose rows mutate is bounded by the read
     instant of the child that shares its clock — the cut of the route named in
     `AS_OF_ROUTES[...]["bounded_by"]`, NOT the latest of its children's cuts
     (5b/5d), with a state-clock start (5c), and it refuses to load at all when
     that cut is missing rather than silently widening to its own task start
     (5e). A param-driven reload holds the same bound (5f).

  6. Snapshot-bound behaviour (t_5a16129f). The insert-clock route of the same
     cluster ends at that same instant (`SNAPSHOT_BOUND_ROUTES`) instead of at its
     own task start, so the rows it loads cannot reference a parent the
     state-bounded window does not carry (6b), while it keeps its own clock and
     watermark (6c) and hands the same instant on (6d). No instant, no load (6e);
     a param-driven reload holds it (6f).
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
# counted with. The header relation's `created_at` is the clock for all five;
# the two item routes join it (neither table has a timestamp of its own).
# `online_orders` left this set in t_5a16129f — its clock is the STATE clock
# (`updated_at`), see STATE_CLOCK_ROUTES below; it is still configured
# incremental on a clock the generator writes in the same statement as the row.
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
    "online_order_items": (
        "/grocery/online/order-items",
        "select max(created_at) from online.orders",
        "select count(*) from online.order_items i join online.orders o using (order_id)"
        " where o.created_at >= %(a)s::timestamptz and o.created_at <= %(b)s::timestamptz",
        "online.order_items",
    ),
}

# The routes whose rows MUTATE after insert watermark on the clock that moves
# with the mutation instead (t_5a16129f, AS_OF_ROUTES in the DAG). Same shape of
# check — configured on its clock, bounded by the child's read instant — but the
# window is anchored on `updated_at`, so a state change is a delta and an old row
# is never left stale.
STATE_CLOCK_ROUTES = {
    "online_orders": (
        "/grocery/online/orders",
        "select max(updated_at) from online.orders",
        "select count(*) from online.orders"
        " where updated_at >= %(a)s::timestamptz and updated_at <= %(b)s::timestamptz",
        "online.orders",
    ),
}

# The insert-clock shape every one of the five must be configured with.
WANT = ("incremental", "created_at", "created_after", "created_before")

# The state-clock shape the mutating tables must be configured with.
WANT_STATE = ("incremental", "updated_at", "updated_after", "updated_before")

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
ROUTE_PARAMS = {"start_dt", "end_dt", "created_after", "created_before",
                "updated_after", "updated_before"}

# (label, routes, the shape their config must have) — the two clock kinds this
# suite guards. Insert clock first: it is the set the rest of the file is about.
def clock_sets():
    return (("insert clock", LIVE_ROUTES, WANT),
            ("state clock", STATE_CLOCK_ROUTES, WANT_STATE))


def as_of_kwargs(mod, task_id, cut_instant, sibling_instant=None):
    """The DAG body's cut wiring for one route, in the form `drive` takes it.

    Empty for the ordinary routes (they take their own task start as the window
    end). For an as-of route: every child it declares as a cut provider, with the
    named `bounded_by` returning `cut_instant` — and any sibling child returning
    `sibling_instant`, which the route must NOT take (see section 5b). For a
    snapshot-bound route: the one route whose instant it ends at (section 6).
    """
    snap = dict(getattr(mod, "SNAPSHOT_BOUND_ROUTES", {}) or {})
    if task_id in snap:
        provider = snap[task_id]
        return {"cut_from": [provider], "cuts": {provider: cut_instant}}
    decl = dict(getattr(mod, "AS_OF_ROUTES", {}) or {})
    if task_id not in decl:
        return {}
    provider = decl[task_id]["bounded_by"]
    children = [c for c, p in mod.REFERENCING_ROUTES if p == task_id]
    cuts = {c: cut_instant for c in children}
    if sibling_instant is not None:
        cuts.update({c: sibling_instant for c in children if c != provider})
    return {"cut_from": children, "cuts": cuts}


def drive(mod, task_id, watermark, params=None, raw_cols=None, log_capture=None,
          cut_from=None, cuts=None, capture=None, expect_error=False):
    """Run ingest_table's window computation for one configured entry, DB + HTTP stubbed.

    The entry is looked up in TABLE_CONFIGS and driven with its own strategy,
    watermark column and bound names, so the test exercises the configuration
    that ships rather than a hand-built tuple. `raw_cols` overrides the raw
    table's column list — that is what decides between the watermark branch and
    the transition fallback.

    `cut_from` / `cuts` emulate the DAG body: the routes this one takes its read
    cut from, and the instant each returned. Without them the cut path is
    skipped entirely (an as-of route refuses to run that way — see section 5).

    Returns the `fetch_params` the loader would have sent to the API. `capture`,
    if given, is filled with the same plus the task's returned read cut and any
    exception raised (with `expect_error`, which is how the as-of refusal is
    asserted).
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

    cf = tuple(cut_from or ())
    cut_by_route = dict(cuts or {})

    class _FakeTask:
        """Only `task_id` is read off a DAG task (the XCom key resolution)."""

        def __init__(self, name):
            self.task_id = f"ingest.{name}"

    class _FakeDag:
        tasks = [_FakeTask(name) for name in sorted(set(cf) | set(cut_by_route))]

    class _FakeTi:
        def xcom_pull(self, task_ids=None, **kwargs):
            # Aligned with the request, the way Airflow returns it.
            return [{"cut": cut_by_route.get(tid.rsplit(".", 1)[-1])}
                    for tid in (task_ids or [])]

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
        result = mod.ingest_table(
            task_id=task_id, api_path=api_path,
            raw_schema=raw_schema, raw_table=raw_table, pk_col=pk_col,
            strategy=strategy, watermark_col=watermark_col,
            api_start_param=api_start_param, api_end_param=api_end_param,
            cut_from=cf, params=params or {},
            dag=_FakeDag(), ti=_FakeTi(),
        )
        captured["cut"] = (result or {}).get("cut")
    except Exception as exc:  # noqa: BLE001 — the caller decides
        if not expect_error:
            raise
        captured["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if log_capture is not None:
            logging.getLogger("gia_watermarks").removeHandler(log_capture)
        for name, fn in saved.items():
            setattr(mod, name, fn)
    if capture is not None:
        capture.update(captured)
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
    sets = clock_sets()

    # ------------------------------------------------------------------
    # 1. configuration — five tables on the insert clock, one on the state clock
    # ------------------------------------------------------------------
    for label, routes, want in sets:
        for task_id in routes:
            cfg = configs.get(task_id)
            check(f"1a {task_id} is configured", cfg is not None)
            if cfg is None:
                continue
            got = (cfg[5], cfg[6], cfg[7], cfg[8])
            check(f"1b {task_id} is incremental on the {label} "
                  f"(strategy, watermark, bounds) == {want}", got == want, f"got={got}")
            check(f"1c {task_id} does not watermark on a backdating date column",
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
    check("1g the as-of declaration names the config the route really has",
          all((configs.get(r, (None,) * 9)[5], configs.get(r, (None,) * 9)[6],
               configs.get(r, (None,) * 9)[7], configs.get(r, (None,) * 9)[8])
              == ("incremental", d.get("state_clock"), *d.get("bounds", ()))
              for r, d in (getattr(gia, "AS_OF_ROUTES", {}) or {}).items()),
          str(getattr(gia, "AS_OF_ROUTES", None)))

    # ------------------------------------------------------------------
    # 2. the bounded reach-back mechanism is retained, and unused
    # ------------------------------------------------------------------
    check("2a no configured table carries a lookback", lookback == {}, str(lookback))

    wm = "2026-09-21T00:59:45.810233-04:00"

    # Retained capability: registered under a table that has no insert clock (so
    # the registration is not a contradiction of check 1f), the branch still
    # shifts the window. Pinned so it cannot rot into dead code unnoticed.
    stand_in = next(t for t, c in configs.items()
                    if c[5] == "incremental"
                    and c[6] not in ("created_at", "updated_at", *BACKDATING)
                    and t not in (getattr(gia, "AS_OF_ROUTES", {}) or {})
                    and t not in (getattr(gia, "SNAPSHOT_BOUND_ROUTES", {}) or {}))
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
    # The end bound is driven with the cut the DAG body would supply: empty for
    # the ordinary routes (they end at their own task start), the child's instant
    # for an as-of route.
    cut_wm = (datetime.fromisoformat(wm) + timedelta(minutes=5)).isoformat()
    for label, routes, want in sets:
        for task_id in routes:
            start_param, end_param = configs[task_id][7], configs[task_id][8]
            got = drive(gia, task_id, wm, **as_of_kwargs(gia, task_id, cut_wm))
            check(f"3a {task_id} fetches [watermark, now] on its configured bounds, "
                  f"unshifted",
                  got.get(start_param) == wm and end_param in got
                  and "start_dt" not in got and "end_dt" not in got,
                  str(got))
            end = got.get(end_param, "")
            check(f"3b {task_id} keeps the end bound after the watermark",
                  end and datetime.fromisoformat(end) > datetime.fromisoformat(wm),
                  f"{end_param}={end}")

    # A populated table whose watermark column has not reached the raw table yet
    # (the first load after an adoption) cannot watermark: it must fall back —
    # visibly — rather than raise UndefinedColumn or load a narrower window.
    # It fired for `pos_return_items` (t_b474c79e) and for the online pair
    # (t_886f7d67); the POS pair was healed before its switch, so it never did.
    # `online_order_events` takes it on the first run after t_5a16129f (its
    # `created_at` is new in the payload) and `online_orders` on its first run
    # after the clock moved to `updated_at`.
    for label, routes, want in sets:
        for task_id in routes:
            cfg = configs[task_id]
            cap = _Capture()
            got = drive(gia, task_id, wm, raw_cols=[cfg[4], "some_date_col"],
                        log_capture=cap, **as_of_kwargs(gia, task_id, cut_wm))
            start_param = cfg[7]
            start = got.get(start_param, "")
            lo = datetime.now(timezone.utc) - timedelta(days=gia.INCREMENTAL_FALLBACK_DAYS + 1)
            hi = datetime.now(timezone.utc) - timedelta(days=gia.INCREMENTAL_FALLBACK_DAYS - 1)
            check(f"3c {task_id}: a raw table without {cfg[6]} falls back to the "
                  f"bounded horizon instead of raising",
                  start and lo < datetime.fromisoformat(start) < hi,
                  f"{start_param}={start}")
            warned = [r.getMessage() for r in cap.records if r.levelno >= logging.WARNING]
            check(f"3d {task_id}: the fallback is announced at WARNING with the "
                  f"column named",
                  any(cfg[6] in m and "watermark" in m for m in warned),
                  " | ".join(warned)[:200] or "no warning logged")

    # Explicit DAG params still win — the documented full-reload recipe. They
    # now land on the configured bounds (the caveat the config comments carry):
    # harmless for 2000->2100, but it is not a date-column window any more. An
    # as-of route keeps its END at the child's instant (see section 5c) — in a
    # real param run the child is windowed with the same `end_dt`, so its cut is
    # that value and the end is unchanged.
    params = {"start_dt": "2000-01-01T00:00:00", "end_dt": "2100-01-01T00:00:00"}
    for label, routes, want in sets:
        for task_id in routes:
            got = drive(gia, task_id, wm, params=params,
                        **as_of_kwargs(gia, task_id, params["end_dt"],
                                       params["end_dt"]))
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

    for label, routes, want in sets:
        a_param, b_param = want[2], want[3]
        for task_id, (path, mx_sql, cnt_sql, rel) in routes.items():
            if spec is None:
                check(f"4a {path} declares {a_param}/{b_param}", False, spec_err)
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
            check(f"4a {path} declares {a_param}/{b_param}",
                  names is not None and {a_param, b_param} <= names,
                  str(sorted(names)) if names else "route not in the spec")
            check(f"4b {path} still declares start_dt/end_dt (the date-column window)",
                  names is not None and {"start_dt", "end_dt"} <= names,
                  str(sorted(names)) if names else "route not in the spec")
            if label == "state clock":
                # The insert clock stays on the route even though the window
                # moved off it — a reader that wants "rows inserted since" can
                # still ask, and dropping it would be a source-contract change.
                check(f"4d {path} still declares created_after/created_before",
                      names is not None
                      and {"created_after", "created_before"} <= names,
                      str(sorted(names)) if names else "route not in the spec")

            if conn is None:
                check(f"4c {path} really filters on {a_param}/{b_param}", False, db_err)
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
                api_30 = api_total(path, {a_param: (mx - timedelta(minutes=30)).isoformat(),
                                          b_param: mx.isoformat()})
                api_old = api_total(path, {a_param: (mx - timedelta(minutes=30)).isoformat(),
                                           b_param: (mx - timedelta(minutes=15)).isoformat()})
                check(f"4c {path} really filters on {a_param}/{b_param} "
                      f"([max-30m, max]: api={api_30} sql={sql_30}; "
                      f"[max-30m, max-15m]: api={api_old} sql={sql_old}; "
                      f"whole table={whole}, tie cluster={sql_tie})",
                      api_30 == sql_30 and api_old == sql_old and api_30 > 0)
            except Exception as exc:  # noqa: BLE001
                check(f"4c {path} really filters on {a_param}/{b_param}", False,
                      f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------
    # 5. as-of behaviour (t_5a16129f) — a route whose rows mutate is bounded by
    #    the read instant of the child that shares its clock, and refuses to run
    #    without it
    # ------------------------------------------------------------------
    as_of = dict(getattr(gia, "AS_OF_ROUTES", {}) or {})
    check("5a the as-of set is declared", bool(as_of), str(as_of))

    for route, decl in sorted(as_of.items()):
        cfg = configs.get(route)
        provider = decl.get("bounded_by")
        end_param = cfg[8] if cfg else None
        # The provider's instant, and a SIBLING child's instant five minutes
        # later: the route must take the provider's, not the latest of its cuts.
        provider_cut = cut_wm
        sibling_cut = (datetime.fromisoformat(wm) + timedelta(minutes=10)).isoformat()
        cap = {}
        got = drive(gia, route, wm,
                    **as_of_kwargs(gia, route, provider_cut, sibling_cut),
                    capture=cap)
        check(f"5b {route}'s window ends at {provider}'s cut, not the latest of "
              f"its children's",
              got.get(end_param) == provider_cut,
              f"{end_param}={got.get(end_param)} provider={provider_cut} "
              f"sibling={sibling_cut}")
        check(f"5c {route}'s window starts at its {decl.get('state_clock')} "
              f"watermark, so a state change on an old row is a delta",
              got.get(cfg[7] if cfg else None) == wm,
              f"{cfg[7] if cfg else None}={got.get(cfg[7] if cfg else None)}")
        check(f"5d {route}'s returned cut is its own window end — the instant the "
              f"routes that reference IT are bounded by",
              cap.get("cut") == provider_cut, f"cut={cap.get('cut')}")
        # No provider cut: the bound would fall back to this task's own start,
        # which is precisely the residual AS_OF_ROUTES exists to close. It has to
        # refuse, not load.
        cap = {}
        drive(gia, route, wm, cut_from=[], capture=cap, expect_error=True)
        check(f"5e {route} refuses to load with no {provider} cut (no silent "
              f"fallback to its own task start)",
              bool(cap.get("error")), f"error={cap.get('error')}")
        # A param-driven reload keeps the same bound (checked in 3e with the real
        # value; here with a param end LATER than the provider's instant, which is
        # what a wide-window run asks for).
        got = drive(gia, route, wm,
                    params={"start_dt": "2000-01-01T00:00:00",
                            "end_dt": "2100-01-01T00:00:00"},
                    **as_of_kwargs(gia, route, provider_cut, sibling_cut))
        check(f"5f {route} holds its end at the as-of instant even on a param-driven "
              f"reload",
              got.get(end_param) == provider_cut, f"{end_param}={got.get(end_param)}")

    # ------------------------------------------------------------------
    # 6. snapshot-bound behaviour (t_5a16129f) — the insert-clock route of the
    #    same cluster ends at the state route's instant too, so an item it loads
    #    cannot reference an order the state-bounded window does not carry
    # ------------------------------------------------------------------
    snap = dict(getattr(gia, "SNAPSHOT_BOUND_ROUTES", {}) or {})
    check("6a the snapshot-bound set is declared", bool(snap), str(snap))

    for route, instant_route in sorted(snap.items()):
        cfg = configs.get(route)
        end_param = cfg[8] if cfg else None
        instant_cut = cut_wm
        cap = {}
        got = drive(gia, route, wm,
                    **as_of_kwargs(gia, route, instant_cut), capture=cap)
        # Not its own task start: the bound IS the instant, so the row it loads
        # and the row the state route loaded were read at one point in time.
        check(f"6b {route}'s window ends at {instant_route}'s read instant, not its "
              f"own task start",
              got.get(end_param) == instant_cut,
              f"{end_param}={got.get(end_param)} instant={instant_cut}")
        # ... while its own clock is untouched: it still loads and watermarks on
        # the column its config names (a row after the instant is delayed, not
        # lost — its insert clock is above the next watermark).
        check(f"6c {route} still windows on its own clock ({cfg[6] if cfg else None}) "
              f"from its own watermark",
              cfg is not None and got.get(cfg[7]) == wm,
              f"{cfg[7] if cfg else None}={got.get(cfg[7] if cfg else None)}")
        check(f"6d {route}'s returned cut is that same instant — the cluster's one "
              f"instant",
              cap.get("cut") == instant_cut, f"cut={cap.get('cut')}")
        # No cut from the instant route: the bound would fall back to this
        # task's own start, which is what puts an order outside the window its
        # item is in. It has to refuse, not load.
        cap = {}
        drive(gia, route, wm, cut_from=[], capture=cap, expect_error=True)
        check(f"6e {route} refuses to load with no {instant_route} cut (no silent "
              f"fallback to its own task start)",
              bool(cap.get("error")), f"error={cap.get('error')}")
        got = drive(gia, route, wm,
                    params={"start_dt": "2000-01-01T00:00:00",
                            "end_dt": "2100-01-01T00:00:00"},
                    **as_of_kwargs(gia, route, instant_cut))
        check(f"6f {route} holds its end at the instant even on a param-driven reload",
              got.get(end_param) == instant_cut, f"{end_param}={got.get(end_param)}")

    if conn is not None:
        conn.close()

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED: " + ", ".join(FAIL))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
