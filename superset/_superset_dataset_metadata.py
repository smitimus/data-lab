#!/usr/bin/env python3
"""
Shared Superset helper: keep a dataset's column metadata in step with the EDW.

A Superset dataset is a SNAPSHOT taken when it is registered: its column list
lives in `table_columns` and is never re-read on its own. The marts here are
CTAS-built on every dbt run, so a mart gaining a column is a ROUTINE event — and
after it the dataset keeps the old list until someone refreshes it.

That is not cosmetic. The seeds point `main_dttm_col` at a column that only
exists after the dbt rebuild (the snapshot `as_of_date` on
mart_hourly_sales_pattern), so a stale dataset makes every chart on it return
`400` in the browser while the chart API, the DB gates and `full-cycle --verify`
all stay green. The only thing that catches it today is the browser DOM scan.

Use it in any seed script right after it resolves a dataset and BEFORE it binds a
chart or sets main_dttm_col:

    from _superset_dataset_metadata import refresh_datasets
    ok, failed = refresh_datasets(token, base_url, datasets_by_table)

Standalone sweep, to heal an instance without re-running a whole seed:

    python3 _superset_dataset_metadata.py --refresh-all

Detection (does every dataset agree with the EDW?) is deliberately NOT here: that
comparison needs the EDW's information_schema and the Superset meta DB at once,
so it lives in `verify_dataset_metadata.sh`, which reads both through the
postgres container — no API, no chart, and it works while Superset is down.
"""

import argparse
import json
import sys
from urllib.parse import urljoin

import requests

SUPERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin"
DEFAULT_SCHEMA = "mart"


def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get_token(base_url=SUPERSET_URL, username=USERNAME, password=PASSWORD):
    r = requests.post(urljoin(base_url, "/api/v1/security/login"),
                      json={"username": username, "password": password,
                            "provider": "db"}, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def find_grocery_db_id(token, base_url=SUPERSET_URL, name="Grocery"):
    """The Superset database id for the EDW, by name."""
    r = requests.get(urljoin(base_url, "/api/v1/database/"),
                     headers=headers(token), timeout=15)
    r.raise_for_status()
    for db in r.json().get("result", []):
        if db.get("database_name") == name:
            return db["id"]
    return None


def list_mart_datasets(token, base_url=SUPERSET_URL, db_id=None,
                       schema=DEFAULT_SCHEMA):
    """Every registered dataset in `schema`, as {table_name: dataset_id}.

    The list endpoint ignores plain `page`/`page_size` params — the RISON `q`
    form is the one it honours — so paging goes through `q` and walks until the
    page comes back short. A listing that silently returns only the first page is
    how a seed ends up binding charts to whatever it happened to see.
    """
    found = {}
    page = 0
    page_size = 200
    while True:
        r = requests.get(
            urljoin(base_url, "/api/v1/dataset/"),
            headers=headers(token),
            params={"q": json.dumps({"page": page, "page_size": page_size})},
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json().get("result", [])
        for d in rows:
            if d.get("schema") != schema:
                continue
            if db_id is not None and (d.get("database") or {}).get("id") != db_id:
                continue
            found[d.get("table_name")] = d["id"]
        if len(rows) < page_size:
            return found
        page += 1


def dataset_columns(token, base_url, ds_id):
    """The column names the dataset exposes right now (a set; empty on error)."""
    try:
        r = requests.get(urljoin(base_url, f"/api/v1/dataset/{ds_id}"),
                         headers=headers(token), timeout=30)
        if r.status_code != 200:
            return set()
        result = r.json().get("result", {})
        return {c.get("column_name") for c in result.get("columns", []) if c.get("column_name")}
    except Exception:
        return set()


def refresh_dataset(token, base_url, ds_id, table_name=None, schema=DEFAULT_SCHEMA):
    """Re-sync a dataset's column metadata from the physical table.

    Returns True when the dataset now exposes the table's current columns. A
    failure is reported, never raised: a seed must still build its charts (and
    say so) rather than die on an instance where one mart is missing.
    """
    label = f" ({schema}.{table_name})" if table_name else ""
    try:
        resp = requests.put(urljoin(base_url, f"/api/v1/dataset/{ds_id}/refresh"),
                            headers=headers(token), timeout=120)
    except Exception as e:
        print(f"  ⚠ Could not refresh dataset {ds_id}{label}: {e}")
        return False
    if resp.status_code == 200:
        # The refresh response carries no column list — read it back so the log
        # shows what the dataset exposes now.
        cols = dataset_columns(token, base_url, ds_id)
        print(f"  ✓ Refreshed dataset {ds_id}{label} — {len(cols)} columns")
        return True
    print(f"  ⚠ Could not refresh dataset {ds_id}{label}: "
          f"{resp.status_code} {resp.text[:120]}")
    return False


def refresh_datasets(token, base_url, datasets, schema=DEFAULT_SCHEMA):
    """Refresh every {table_name: dataset_id} entry. Returns (ok, failed).

    `datasets` may be a dict (table -> id) or an iterable of ids; the dict form
    is what the seeds hold after resolving datasets by name.
    """
    if isinstance(datasets, dict):
        items = sorted(datasets.items())
    else:
        items = [(None, ds_id) for ds_id in datasets]
    ok = failed = 0
    for table_name, ds_id in items:
        if not ds_id:
            continue
        if refresh_dataset(token, base_url, ds_id, table_name, schema):
            ok += 1
        else:
            failed += 1
    return ok, failed


def main():
    parser = argparse.ArgumentParser(
        description="Sweep and heal Superset dataset column metadata from the EDW")
    parser.add_argument("--superset-url", default=SUPERSET_URL)
    parser.add_argument("--username", default=USERNAME)
    parser.add_argument("--password", default=PASSWORD)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--refresh-all", action="store_true",
                        help="refresh every dataset in the schema (the only action)")
    args = parser.parse_args()
    if not args.refresh_all:
        parser.error("nothing to do — pass --refresh-all")

    token = get_token(args.superset_url, args.username, args.password)
    db_id = find_grocery_db_id(token, args.superset_url)
    if db_id is None:
        print("✗ no 'Grocery' database in Superset — nothing to refresh")
        return 1
    datasets = list_mart_datasets(token, args.superset_url, db_id, args.schema)
    print(f"{args.schema} datasets: {len(datasets)}")
    ok, failed = refresh_datasets(token, args.superset_url, datasets, args.schema)
    print(f"refreshed {ok}, failed {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
