#!/usr/bin/env python3
"""
Schema-drift + partial-load tests for grocery_ingest_api.py
============================================================
Run INSIDE the airflow-worker container (needs airflow, psycopg2, requests,
and network reach to `postgres`):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_schema_drift.py

No pytest required — plain asserts, exit code 0 = pass.

Tests:
  1. New verisim column mid-flight: raw table exists without the column ->
     ingest ALTERs it in, loads the data, logs SCHEMA DRIFT (end-to-end).
  2. Raw column vanished from API: logged, ingest still succeeds.
  3. Persistent 5xx from API: ingest raises RuntimeError after retries
     (no more silent partial-load-as-success).
  4. Transient 5xx then recovery: retries absorb it, data loads.
  5. Fewer rows than API `total`: raises instead of loading partial data.
"""
from __future__ import annotations

import importlib.util
import logging
import sys
import types

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DAG_PATH = "/opt/airflow/dags/grocery_ingest_api.py"
spec = importlib.util.spec_from_file_location("gia", DAG_PATH)
gia = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gia)

# Retry backoff would slow tests down; keep attempt count but zero the sleeps.
gia.FETCH_RETRY_BACKOFF = 0.001

CONN = dict(gia.EDW_CONN)
SCHEMA = "raw_drift_test"


class FakeResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


def install_api(routes):
    """routes: list of callables(params) -> FakeResp, consumed per path in order
    of first sight; a string '500' entry always returns 500."""
    state = {"calls": []}

    def fake_get(url, params=None, timeout=None):
        state["calls"].append((url, dict(params or {})))
        path = url.replace(gia.API_BASE, "")
        seq = routes.get(path)
        key = (path, len([c for c in state["calls"] if c[0].endswith(path)]) - 1)
        handler = seq[key[1]] if key[1] < len(seq) else seq[-1]
        resp = handler(params or {})
        return resp

    orig = gia.requests.get
    gia.requests.get = fake_get
    return state, lambda: setattr(gia.requests, "get", orig)


def psql(sql):
    conn = psycopg2.connect(**CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
        try:
            return cur.fetchall()
        except Exception:
            return None


def run_ingest(task_id, api_path, raw_table, pk_col="id", strategy="full", watermark_col=None):
    gia.ingest_table(
        task_id=task_id, api_path=api_path, raw_schema=SCHEMA,
        raw_table=raw_table, pk_col=pk_col, strategy=strategy,
        watermark_col=watermark_col, api_start_param="start_dt", api_end_param="end_dt",
        params={},
    )


def setup_table(raw_table, cols, rows_sql=None):
    psql(f'DROP TABLE IF EXISTS "{SCHEMA}"."{raw_table}"')
    col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
    psql(f'CREATE TABLE "{SCHEMA}"."{raw_table}" ({col_defs}, '
         f'"_sdc_extracted_at" TEXT, "_sdc_batched_at" TEXT, "_sdc_deleted_at" TEXT, '
         f'PRIMARY KEY ("id"))')
    if rows_sql:
        psql(rows_sql)


def cols_of(raw_table):
    conn = psycopg2.connect(**CONN)
    out = gia._raw_columns(conn, SCHEMA, raw_table)
    conn.close()
    return out


PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS " if cond else "FAIL ") + name + (f" — {detail}" if detail else ""))


def main():
    psql(f"CREATE SCHEMA IF NOT EXISTS \"{SCHEMA}\"")

    # ------------------------------------------------------------------
    # 1. New verisim column appears mid-flight -> ALTER + load + warn
    # ------------------------------------------------------------------
    setup_table("t_newcol", ["id", "name"],
                "INSERT INTO raw_drift_test.t_newcol (id, name) VALUES ('1','a')")
    rows = [
        {"id": "1", "name": "a", "loyalty_tier": "gold"},
        {"id": "2", "name": "b", "loyalty_tier": "silver"},
    ]
    state, restore = install_api({"/fake/newcol": [lambda p: FakeResp({"data": rows, "total": 2})]})
    try:
        run_ingest("t_newcol", "/fake/newcol", "t_newcol")
    finally:
        restore()
    got_cols = cols_of("t_newcol")
    check("1a ALTER added loyalty_tier", "loyalty_tier" in got_cols, str(got_cols))
    data = psql('SELECT id, loyalty_tier FROM raw_drift_test.t_newcol ORDER BY id')
    check("1b new column data loaded", data == [("1", "gold"), ("2", "silver")], str(data))

    # ------------------------------------------------------------------
    # 2. Raw column absent from API -> warn, still succeeds, no crash
    # ------------------------------------------------------------------
    setup_table("t_gone", ["id", "name", "vanished_col"])
    rows2 = [{"id": "9", "name": "n"}]
    state, restore = install_api({"/fake/gone": [lambda p: FakeResp({"data": rows2, "total": 1})]})
    try:
        run_ingest("t_gone", "/fake/gone", "t_gone")
        ok2 = True
    except Exception as e:
        ok2 = False
        print("  unexpected:", e)
    finally:
        restore()
    check("2 raw-only column tolerated", ok2)

    # ------------------------------------------------------------------
    # 3. Persistent 5xx -> RuntimeError after retries
    # ------------------------------------------------------------------
    setup_table("t_5xx", ["id", "name"])
    state, restore = install_api({"/fake/fifty": [lambda p: FakeResp({}, 503)] * 10})
    raised = None
    try:
        run_ingest("t_5xx", "/fake/fifty", "t_5xx")
    except RuntimeError as e:
        raised = str(e)
    finally:
        restore()
    check("3 persistent 5xx raises", raised is not None and "failed after" in raised, raised or "no raise")
    check("3b retried FETCH_RETRIES times",
          len([c for c in state["calls"] if c[0].endswith("/fake/fifty")]) == gia.FETCH_RETRIES,
          str(len([c for c in state["calls"] if c[0].endswith("/fake/fifty")])))

    # ------------------------------------------------------------------
    # 4. Transient 5xx then recovery -> data loads
    # ------------------------------------------------------------------
    setup_table("t_flaky", ["id", "name"])
    seq = [lambda p: FakeResp({}, 500),
           lambda p: FakeResp({"data": [{"id": "5", "name": "x"}], "total": 1})]
    state, restore = install_api({"/fake/flaky": seq})
    try:
        run_ingest("t_flaky", "/fake/flaky", "t_flaky")
        ok4 = psql("SELECT count(*) FROM raw_drift_test.t_flaky") == [(1,)]
    finally:
        restore()
    check("4 transient 5xx recovered", ok4)

    # ------------------------------------------------------------------
    # 5. total=2500 but only 1000 rows served -> partial load must raise
    #    (small-page path: max_page_fetch=None forces PAGE_SIZE=1000 loop;
    #     page 1 = 1000 rows (full), page 2 = empty -> shortfall vs total)
    # ------------------------------------------------------------------
    setup_table("t_partial", ["id", "name"])
    big = [{"id": str(i), "name": "n"} for i in range(1000)]
    pages = [lambda p: FakeResp({"data": big, "total": 2500}),
             lambda p: FakeResp({"data": [], "total": 2500})]
    state, restore = install_api({"/fake/partial": pages})
    raised5 = None
    try:
        run_ingest("t_partial", "/fake/partial", "t_partial", strategy="incremental",
                   pk_col="id", watermark_col="name")
    except RuntimeError as e:
        raised5 = str(e)
    finally:
        restore()
    check("5 partial vs advertised total raises",
          raised5 is not None and "refusing partial load" in raised5, raised5 or "no raise")

    psql(f"DROP SCHEMA IF EXISTS \"{SCHEMA}\" CASCADE")
    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed:", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
