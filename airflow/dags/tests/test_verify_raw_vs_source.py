#!/usr/bin/env python3
"""
Raw-vs-source invariant tests for grocery_ingest_api.py
=======================================================
Run INSIDE the airflow-worker container (needs airflow, psycopg2, requests,
network reach to `postgres`, and the source reachable at VERISIM_API_URL /
VERISIM_DB_HOST — the service-name path the DAG ships with):

    docker exec airflow-worker python /opt/airflow/dags/tests/test_verify_raw_vs_source.py

No pytest required — plain asserts, exit code 0 = pass.

Tests:
  1. _reconcile classifies the decision table: excess, equal, shortfall, absent
     raw table, unreadable source relation.
  2. _excess_message names both counts and totals — the message a human reads at
     3am must say which table and by how much.
  3. The 2026-09-21 regression (t_05b48b69), numerically: 1,136,360 raw rows
     against a 98,112-row source must be flagged, and the 11.6x ratio reported.
  4. _assert_api_is_this_source fails when the API and the source DB disagree,
     and _source_counts reports an unreadable relation instead of raising.
  5. End-to-end against the live source: a raw table matching hr.locations
     passes verify_raw_vs_source, one extra row makes it raise, and a mapping
     pointing at a relation the source does not have fails as a contract break.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# Overridable so the suite can be pointed at an un-deployed revision, e.g.
#   docker exec -e GIA_DAG_PATH=/tmp/gia.py airflow-worker python .../this_file.py
DAG_PATH = os.environ.get("GIA_DAG_PATH", "/opt/airflow/dags/grocery_ingest_api.py")
spec = importlib.util.spec_from_file_location("gia", DAG_PATH)
gia = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gia)

SCHEMA = "raw_verify_test"
PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS " if cond else "FAIL ") + name + (f" — {detail}" if detail else ""))


def psql(sql, fetch=False):
    conn = psycopg2.connect(**gia.EDW_CONN)
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


def row(task_id, raw_table, relation, raw_n):
    return (task_id, SCHEMA, raw_table, relation, raw_n)


def main():
    # ------------------------------------------------------------------
    # 1. decision table
    # ------------------------------------------------------------------
    src = {"pos.transactions": 100, "hr.locations": 5, "pos.products": 90, "pos.coupons": 42}
    excess, missing = gia._reconcile(
        [
            row("pos_transactions", "transactions", "pos.transactions", 300),  # excess
            row("hr_locations", "locations", "hr.locations", 5),               # equal
            row("pos_products", "products", "pos.products", 5),                # shortfall
            row("pos_coupons", "coupons", "pos.coupons", None),                # absent
            row("inv_products", "inv_products", "inv.products", 7),            # unreadable
        ],
        src,
    )
    check("1a excess detected",
          [e[0] for e in excess] == ["pos_transactions"], str(excess))
    check("1b excess carries both counts", excess[0][3:] == (300, 100), str(excess[0]))
    check("1c equal / shortfall / absent tolerated", len(excess) == 1, str(excess))
    check("1d unreadable source relation reported", missing == ["inv.products"], str(missing))

    # ------------------------------------------------------------------
    # 2. the failure message a human actually reads
    # ------------------------------------------------------------------
    msg = gia._excess_message(excess, rows_checked=32)
    check("2a message names the offending table",
          "pos.transactions" in msg and "raw_verify_test.transactions" in msg, msg)
    check("2b message reports the ratio and the denominators",
          "300" in msg and "100" in msg and "3.0x" in msg and "32" in msg, msg)

    # ------------------------------------------------------------------
    # 3. the t_05b48b69 regression, numerically
    # ------------------------------------------------------------------
    inc_excess, _ = gia._reconcile(
        [row("pos_transactions", "transactions", "pos.transactions", 1136360)],
        {"pos.transactions": 98112},
    )
    check("3a foreign-instance load is flagged", len(inc_excess) == 1, str(inc_excess))
    check("3b ratio reported as 11.6x",
          "11.6x" in gia._excess_message(inc_excess, rows_checked=32),
          gia._excess_message(inc_excess, rows_checked=32))
    ok_excess, _ = gia._reconcile(
        [row("pos_transactions", "transactions", "pos.transactions", 98112)],
        {"pos.transactions": 98200},  # source grew during the load
    )
    check("3c a faithful load of the same table passes", not ok_excess, str(ok_excess))

    # ------------------------------------------------------------------
    # 4. split-horizon source address (API and DB are different instances)
    # ------------------------------------------------------------------
    real_source_counts = gia._source_counts
    try:
        gia._source_counts = lambda rels: ({r: 999 for r in rels}, {})
        raised = None
        try:
            gia._assert_api_is_this_source()
        except RuntimeError as e:
            raised = str(e)
        check("4a mismatched API/DB raises", raised is not None and "999" in raised,
              raised or "no raise")
        check("4b mismatch message names both addresses",
              raised is not None
              and gia.VERISIM_API_URL in raised
              and gia.VERISIM_DB["host"] in raised,
              raised or "no raise")
    finally:
        gia._source_counts = real_source_counts

    counts, errors = gia._source_counts(["hr.locations", "pos.no_such_table"])
    check("4c unreadable relation reported, not raised",
          counts["hr.locations"] is not None
          and counts["pos.no_such_table"] is None
          and "pos.no_such_table" in errors,
          str(errors))
    check("4d connection still usable after a failed count",
          counts["hr.locations"] == gia._source_counts(["hr.locations"])[0]["hr.locations"],
          str(counts))

    # ------------------------------------------------------------------
    # 5. end-to-end against the live source
    # ------------------------------------------------------------------
    source_counts, source_errors = gia._source_counts(["hr.locations"])
    source_n = source_counts["hr.locations"]
    check("5a live source reachable", not source_errors and source_n > 0,
          f"hr.locations={source_n} errors={source_errors}")

    gia.TABLE_CONFIGS = [
        ("hr_locations", "/grocery/hr/locations", SCHEMA, "locations", "location_id",
         "full", None, None, None)
    ]
    gia.SOURCE_RELATIONS = {"hr_locations": "hr.locations"}

    psql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    psql(f'CREATE SCHEMA "{SCHEMA}"')
    psql(f'CREATE TABLE "{SCHEMA}"."locations" ("location_id" TEXT, '
         f'"_sdc_extracted_at" TEXT, "_sdc_batched_at" TEXT, "_sdc_deleted_at" TEXT, '
         f'PRIMARY KEY ("location_id"))')
    psql(f'INSERT INTO "{SCHEMA}"."locations" (location_id) '
         f'SELECT \'loc-\' || g FROM generate_series(1, {source_n}) g')

    passed = True
    try:
        gia.verify_raw_vs_source()
    except Exception as e:
        passed = False
        print("  unexpected:", e)
    check("5b matching raw/source passes the invariant", passed)

    psql(f'INSERT INTO "{SCHEMA}"."locations" (location_id) VALUES (\'loc-extra\')')
    raised5 = None
    try:
        gia.verify_raw_vs_source()
    except RuntimeError as e:
        raised5 = str(e)
    check("5c one excess row fails the invariant",
          raised5 is not None and f"{SCHEMA}.locations" in raised5,
          raised5 or "no raise")

    psql(f'DELETE FROM "{SCHEMA}"."locations" WHERE location_id = \'loc-extra\'')

    # a mapping pointing at a relation the source does not have is a contract
    # break, and must read as one instead of bubbling a psycopg2 error
    gia.SOURCE_RELATIONS = {"hr_locations": "pos.no_such_table"}
    raised6 = None
    try:
        gia.verify_raw_vs_source()
    except RuntimeError as e:
        raised6 = str(e)
    check("5d relation missing from the source is a named contract break",
          raised6 is not None and "pos.no_such_table" in raised6
          and "contract broken" in raised6,
          raised6 or "no raise")

    psql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed:", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
