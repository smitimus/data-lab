#!/usr/bin/env python3
"""
Ingest serialisation tests (t_657cebc3)
=======================================
Run INSIDE the airflow-worker container (needs airflow, psycopg2, and reach to
`postgres`):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_ingest_serialization.py

No pytest required — plain asserts, exit code 0 = pass.

What it proves, and why each part is load-bearing:

  1. Configuration. `grocery_ingest_api` and `grocery_complete_pipeline` both
     declare `max_active_runs=1`, and the pipeline's `ingest`/`transform` steps
     wait for the child run. Two runs of either DAG in flight means two loads in
     flight, which is what destroyed the dev slot on 2026-09-21.
  2. Structure. `ingest_table` takes the per-table lock *before* the DROP /
     TRUNCATE, and every callable in the DAG is a loader that does. A refactor
     that moves the acquire below the destructive statement passes every other
     test in this repo and reintroduces the bug, so it is asserted here.
  3. Behaviour, live against the EDW:
       - two loaders cannot hold one table: the second waits, then raises with
         the table and the holder named;
       - the lock survives a commit (it is session-scoped, and the loader commits
         once per page — a transaction-scoped lock would be released at exactly
         the moment the table is half-full);
       - different tables do not block each other (pool concurrency still pays);
       - the lock dies with the holder's connection, so a killed worker cannot
         leave a table locked.
"""
from __future__ import annotations

import importlib.util
import inspect
import logging
import os
import sys
import time

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

DAGS_DIR = os.environ.get("DAGS_DIR", "/opt/airflow/dags")
sys.path.insert(0, DAGS_DIR)

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


def psql(mod, sql, fetch=False):
    conn = mod._edw_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if fetch:
                try:
                    return cur.fetchall()
                except Exception:
                    return None
    finally:
        conn.close()


def table_exists(mod, schema, table):
    rows = psql(mod, "select 1 from information_schema.tables "
                     f"where table_schema = '{schema}' and table_name = '{table}'",
                fetch=True)
    return bool(rows)


def table_rows(mod, schema, table):
    rows = psql(mod, f'select count(*) from "{schema}"."{table}"', fetch=True)
    return rows[0][0] if rows else 0


def try_acquire(mod, conn, schema, table, task_id, wait_s, poll_s):
    """Acquire with a short, test-sized bounded wait. Returns (error, seconds)."""
    real = (mod.INGEST_LOCK_WAIT_S, mod.INGEST_LOCK_POLL_S)
    mod.INGEST_LOCK_WAIT_S, mod.INGEST_LOCK_POLL_S = wait_s, poll_s
    started = time.monotonic()
    error = None
    try:
        mod._acquire_table_lock(conn, schema, table, task_id)
    except RuntimeError as exc:
        error = str(exc)
    finally:
        mod.INGEST_LOCK_WAIT_S, mod.INGEST_LOCK_POLL_S = real
    return error, time.monotonic() - started


def main():
    gia = load(f"{DAGS_DIR}/grocery_ingest_api.py", "gia_serial")
    gcp = load(f"{DAGS_DIR}/grocery_complete_pipeline.py", "gcp_serial")

    # ------------------------------------------------------------------
    # 1. the scheduler-level mutex (one DagRun at a time, per DAG)
    # ------------------------------------------------------------------
    check("1a grocery_ingest_api max_active_runs=1",
          gia.dag.max_active_runs == 1, str(gia.dag.max_active_runs))
    check("1b grocery_complete_pipeline max_active_runs=1",
          gcp.dag.max_active_runs == 1, str(gcp.dag.max_active_runs))

    waits = {t.task_id: getattr(t, "wait_for_completion", None)
             for t in gcp.dag.tasks if t.task_id in ("ingest", "transform")}
    check("1c the pipeline waits for each child run",
          waits == {"ingest": True, "transform": True}, str(waits))

    # ------------------------------------------------------------------
    # 2. every ingest task goes through the locking loader
    # ------------------------------------------------------------------
    ingest_src = inspect.getsource(gia.ingest_table)
    # Statement order, not comment order: comments and docstrings mentioning the
    # destructive statements would otherwise sort ahead of the lock call.
    ingest_code = "\n".join(
        line for line in ingest_src.splitlines() if not line.strip().startswith("#")
    )
    acquire_at = ingest_code.find("_acquire_table_lock")
    destructive = [p for p in (ingest_code.find("TRUNCATE"), ingest_code.find("DROP TABLE"))
                   if p >= 0]
    first_destructive = min(destructive) if destructive else -1
    check("2a ingest_table takes the per-table lock", acquire_at >= 0)
    check("2b the lock is taken before the DROP/TRUNCATE",
          acquire_at >= 0 and 0 <= acquire_at < first_destructive,
          f"lock@{acquire_at} destructive@{first_destructive}")

    unlocked = []
    for task in gia.dag.tasks:
        callable_ = getattr(task, "python_callable", None)
        if callable_ is None:
            continue
        if getattr(callable_, "__name__", "") in ("ingest_table", "verify_raw_vs_source"):
            continue
        unlocked.append(f"{task.task_id}={getattr(callable_, '__name__', callable_)}")
    check("2c no unlocked callable sneaked into the ingest DAG", not unlocked, str(unlocked))

    # ------------------------------------------------------------------
    # 3. the lock itself, live
    # ------------------------------------------------------------------
    schema, table, other = "raw_lock_probe", "single_writer", "other_probe"
    key = gia._ingest_lock_key(schema, table)

    holder = gia._edw_conn()
    second = gia._edw_conn()
    try:
        # name the sessions so the diagnostic can be asserted, not just eyeballed
        for conn, name in ((holder, "serial-test-holder"), (second, "serial-test-second")):
            with conn.cursor() as cur:
                cur.execute("set application_name = %s", (name,))
            conn.commit()

        gia._acquire_table_lock(holder, schema, table, "test-holder")
        holder.commit()  # session-scoped: must survive the loader's per-page commit

        error, elapsed = try_acquire(gia, second, schema, table, "test-second", 2, 1)
        check("3a a second loader cannot take a held table",
              error is not None, error or "no raise — it acquired the lock")
        check("3b it waits its bounded time, then fails",
              1.5 <= elapsed <= 8, f"waited {elapsed:.1f}s")
        check("3c the failure names the table, the wait and the holder",
              error is not None
              and key in error
              and "more than 2s" in error
              and "serial-test-holder" in error,
              error or "no raise")

        error_other, elapsed_other = try_acquire(
            gia, second, schema, other, "test-other", 2, 1)
        check("3d a different table acquires immediately",
              error_other is None and elapsed_other < 5,
              error_other or f"{elapsed_other:.1f}s")

        holder.close()  # the lock dies with the holder's connection
        error_after, elapsed_after = try_acquire(
            gia, second, schema, table, "test-second", 5, 1)
        check("3e releasing the holder frees the table",
              error_after is None, error_after or f"{elapsed_after:.1f}s")

        desc = gia._holder_of_table_lock(second, key)
        check("3f the holder diagnostic names a pid or declines cleanly",
              "pid" in desc or "unknown" in desc or "not in pg_locks" in desc, desc)

        with second.cursor() as cur:
            cur.execute("select 1")
            check("3g the caller's session survives the diagnostic",
                  cur.fetchone()[0] == 1)
    finally:
        try:
            holder.close()
        except Exception:
            pass
        second.close()

    # ------------------------------------------------------------------
    # 4. the production loader, run at a table someone else is loading
    #    (writes only into the probe schema, then cleans it up)
    # ------------------------------------------------------------------
    probe = {"task_id": "probe_locations", "api_path": "/grocery/hr/locations",
             "raw_schema": schema, "raw_table": "probe_locations",
             "pk_col": "location_id", "strategy": "full", "watermark_col": None,
             "api_start_param": None, "api_end_param": None}

    guard = gia._edw_conn()
    try:
        with guard.cursor() as cur:
            cur.execute("set application_name = %s", ("serial-test-guard",))
        guard.commit()
        gia._acquire_table_lock(guard, schema, probe["raw_table"], "test-guard")

        real_wait = gia.INGEST_LOCK_WAIT_S
        gia.INGEST_LOCK_WAIT_S = 2
        refused, blocked_for = None, None
        started = time.monotonic()
        try:
            gia.ingest_table(**probe, params={})
        except RuntimeError as exc:
            refused = str(exc)
        finally:
            blocked_for = time.monotonic() - started
            gia.INGEST_LOCK_WAIT_S = real_wait

        check("4a a real ingest task refuses to load a table another loader holds",
              refused is not None and schema in refused,
              refused or "no raise — it loaded the table")
        check("4b it refuses before touching the table, not after",
              not table_exists(gia, schema, probe["raw_table"]),
              f"waited {blocked_for:.1f}s; "
              "the probe table was created while the lock was held elsewhere")
    finally:
        guard.close()

    # and the same call succeeds once nobody else holds the table
    try:
        gia.ingest_table(**probe, params={})
        loaded = table_rows(gia, schema, probe["raw_table"])
        check("4c the loader works normally when the table is free", loaded > 0, str(loaded))
    except Exception as exc:  # noqa: BLE001 — reported as a failure
        check("4c the loader works normally when the table is free", False, repr(exc))

    psql(gia, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed:", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
