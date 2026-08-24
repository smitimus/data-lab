"""
export_exec_report.py — data-lab#35/#36 daily executive report export.

Pulls the 8 charts on the "Store — Executive Performance Overview"
dashboard from Superset and writes each as a CSV into the MinIO
S3 bucket `reports/`, under a dated prefix. Runs inside the Airflow
worker (which shares `postgres_network` with both `superset` and
`minio`).

The dashboard is resolved BY TITLE, not by hardcoded id — Superset
ids drift across seeds/reseeds (dashboard id 13 became id 10 after
the 2026-08 reseed and broke this DAG with a 404). Set DASHBOARD_ID
env to pin an id explicitly if the title lookup ever matches more
than one dashboard.

Why the SQL Lab path (not /api/v1/chart/data):
  Superset 4.1.2's /chart/data endpoint rejects programmatic
  payloads with a server-side `QueryContextFactory.create() missing
  'datasource'` TypeError. SQL Lab's /sql_lab/execute/ endpoint
  works reliably, so we resolve each chart -> dataset -> table and run a
  SELECT through SQL Lab, then stream the result rows to CSV.

Auth: Superset login with admin/admin (demo creds, from airflow/.env
is NOT used here — the Superset admin user is fixed in setup.py).
"""
from __future__ import annotations

import csv
import io
import json
import os
from datetime import datetime, timezone

import boto3
import requests
from botocore.client import Config

# ---------------------------------------------------------------------------
# Config (injected via airflow/.env -> container env)
# ---------------------------------------------------------------------------
SUPERSET_URL = "http://superset:8088"
SUPERSET_USER = "admin"
SUPERSET_PASS = "admin"

# Dashboard resolved by title by default (ids drift across reseeds; the old
# hardcoded id 13 went 404 after the 2026-08 reseed). DASHBOARD_ID env pins
# a specific id when needed.
DASHBOARD_TITLE = os.environ.get(
    "DASHBOARD_TITLE", "Store — Executive Performance Overview"
)
DASHBOARD_ID = (
    int(os.environ["DASHBOARD_ID"]) if os.environ.get("DASHBOARD_ID") else None
)

# Dev defaults — overridable via env (S3_*). Change before any non-local deploy.
# MinIO enforces an 8-char minimum on the root password, so the "admin/admin"
# default uses `adminadmin` for the secret (a literal `admin` would fail to start).
S3_ENDPOINT = "http://minio:9000"     # MinIO service URL (Docker DNS on postgres_network)
S3_ACCESS_KEY = "admin"                    # MinIO root user (default)
S3_SECRET_KEY = "adminadmin"                # MinIO root password (default; >=8 chars required)
S3_BUCKET = "reports"                    # bucket for exported reports
S3_REGION = "us-east-1"                  # bucket region (cosmetic for MinIO)
DATABASE_ID = 1  # the Grocery (EDW) database id in Superset


def _superset_token() -> str:
    r = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _get_json_result(r: requests.Response, what: str) -> dict:
    """Return response JSON's `result`, raising a clear error otherwise."""
    if r.status_code != 200:
        raise RuntimeError(f"{what}: Superset returned HTTP {r.status_code}: {r.text[:300]}")
    body = r.json()
    if "result" not in body:
        raise RuntimeError(f"{what}: Superset response has no 'result' key: {str(body)[:300]}")
    return body["result"]


def _dashboard_id(token: str) -> int:
    """Resolve the exec dashboard id by title (ids drift across reseeds)."""
    if DASHBOARD_ID is not None:
        return DASHBOARD_ID
    h = {"Authorization": f"Bearer {token}"}
    r = requests.get(
        f"{SUPERSET_URL}/api/v1/dashboard/?q=(page_size:100)",
        headers=h, timeout=20,
    )
    if r.status_code != 200:
        raise RuntimeError(f"dashboard list: Superset returned HTTP {r.status_code}: {r.text[:300]}")
    for d in r.json().get("result", []):
        if d.get("dashboard_title") == DASHBOARD_TITLE:
            return int(d["id"])
    titles = ", ".join(str(d.get("dashboard_title")) for d in r.json().get("result", []))[:400]
    raise RuntimeError(
        f"dashboard '{DASHBOARD_TITLE}' not found in Superset. "
        f"Available dashboards: {titles}"
    )


def _chart_table_map(token: str, dashboard_id: int) -> list[dict]:
    """Return [{chart_id, slice_name, schema, table, sql}] for the exec dashboard."""
    h = {"Authorization": f"Bearer {token}"}
    dash = _get_json_result(
        requests.get(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}",
                     headers=h, timeout=20),
        f"dashboard {dashboard_id}",
    )
    pj = json.loads(dash["position_json"])
    chart_ids = sorted(
        {v["meta"]["chartId"] for v in pj.values()
         if isinstance(v, dict) and v.get("type") == "CHART"}
    )
    out = []
    for cid in chart_ids:
        meta = _get_json_result(
            requests.get(f"{SUPERSET_URL}/api/v1/chart/{cid}",
                         headers=h, timeout=20),
            f"chart {cid}",
        )
        ds_raw = meta.get("datasource") or (
            json.loads(meta["params"]).get("datasource") if meta.get("params") else None
        )
        ds_id = int(str(ds_raw).split("__")[0])
        ds = _get_json_result(
            requests.get(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}",
                         headers=h, timeout=20),
            f"dataset {ds_id} (chart {cid})",
        )
        schema = ds.get("schema") or "grocery"
        table = ds["table_name"]
        out.append({
            "chart_id": cid,
            "slice_name": meta.get("slice_name", f"chart_{cid}"),
            "schema": schema,
            "table": table,
            "sql": f"SELECT * FROM {schema}.{table} LIMIT 10000",
        })
    return out


def _run_sql(token: str, sql: str, schema: str) -> list[dict]:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.post(
        f"{SUPERSET_URL}/api/v1/sqllab/execute/",
        headers=h,
        json={"database_id": DATABASE_ID, "sql": sql, "schema": schema,
              "sql_editor_id": "1", "queryLimit": 10000},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()["data"]


def _rows_to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for row in rows:
            w.writerow(row)
    return buf.getvalue().encode("utf-8")


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(signature_version="s3v4"),
    )


def export() -> dict:
    token = _superset_token()
    dash_id = _dashboard_id(token)
    charts = _chart_table_map(token, dash_id)
    s3 = _s3_client()
    # safe object-key prefix (no spaces/slashes)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = f"exec-report/{day}"

    written = []
    for c in charts:
        rows = _run_sql(token, c["sql"], c["schema"])
        key = f"{prefix}/{c['chart_id']:03d}_{c['slice_name'].replace(' ', '_').lower()}.csv"
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=_rows_to_csv(rows),
                      ContentType="text/csv")
        written.append({"chart": c["slice_name"], "rows": len(rows), "key": key})

    # also drop a manifest
    manifest = {
        "dashboard": dash_id,
        "dashboard_title": DASHBOARD_TITLE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": written,
    }
    s3.put_object(Bucket=S3_BUCKET, Key=f"{prefix}/manifest.json",
                  Body=json.dumps(manifest, indent=2).encode(),
                  ContentType="application/json")
    return {"prefix": prefix, "files": written}


if __name__ == "__main__":
    import os
    # Allow local dev override via env (so this script can be smoke-tested
    # outside Airflow with real creds in the shell env).
    S3_ENDPOINT = os.environ.get("S3_ENDPOINT", S3_ENDPOINT)
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", S3_ACCESS_KEY)
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", S3_SECRET_KEY)
    S3_BUCKET = os.environ.get("S3_BUCKET", S3_BUCKET)
    S3_REGION = os.environ.get("S3_REGION", S3_REGION)
    print(json.dumps(export(), indent=2))
