"""
export_exec_report.py — data-lab#35/#36 daily executive report export.

Pulls the 8 charts on dashboard 13 ("Store — Executive Performance
Overview") from Superset and writes each as a CSV into the MinIO
S3 bucket `reports/`, under a dated prefix. Runs inside the Airflow
worker (which shares `postgres_network` with both `superset` and
`minio`).

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
DASHBOARD_ID = 13

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


def _chart_table_map(token: str) -> list[dict]:
    """Return [{chart_id, slice_name, schema, table, sql}] for dashboard 13."""
    h = {"Authorization": f"Bearer {token}"}
    dash = requests.get(f"{SUPERSET_URL}/api/v1/dashboard/{DASHBOARD_ID}",
                       headers=h, timeout=20).json()["result"]
    pj = json.loads(dash["position_json"])
    chart_ids = sorted(
        {v["meta"]["chartId"] for v in pj.values()
         if isinstance(v, dict) and v.get("type") == "CHART"}
    )
    out = []
    for cid in chart_ids:
        meta = requests.get(f"{SUPERSET_URL}/api/v1/chart/{cid}",
                            headers=h, timeout=20).json()["result"]
        ds_raw = meta.get("datasource") or (
            json.loads(meta["params"]).get("datasource") if meta.get("params") else None
        )
        ds_id = int(str(ds_raw).split("__")[0])
        ds = requests.get(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}",
                         headers=h, timeout=20).json()["result"]
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
    charts = _chart_table_map(token)
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
        "dashboard": DASHBOARD_ID,
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
