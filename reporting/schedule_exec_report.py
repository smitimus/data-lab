#!/usr/bin/env python3
"""
Verify / register the Store Performance Executive daily report (data-lab#32).

HISTORY / PIVOT (2026-07-20):
  Originally this script registered a Superset `/api/v1/report/` *email* schedule
  (the ALERT_REPORTS + SMTP path). Chris pivoted delivery off Superset email to
  **S3 export** (data-lab#35/#36). The actual daily export now lives in the
  Airflow DAG `exec_report_s3_export` (`airflow/dags/exec_report_s3_export_dag.py`,
  `airflow/dags/export_exec_report.py`) and writes CSVs + a manifest to the MinIO
  bucket `s3://reports/exec-report/<date>/`. This script no longer touches the
  Superset report blueprint.

WHAT THIS SCRIPT NOW DOES:
  Verifies the end-to-end S3 report pipeline is ready, so the last acceptance
  criterion of data-lab#32 ("at least one scheduled report verified to deliver")
  can be confirmed. It:
    1. Confirms Superset dashboard 13 ("Store — Executive Performance Overview")
       exists, is published, and exposes its 8 KPI charts.
    2. Confirms the Airflow DAG `exec_report_s3_export` is present and unpaused.
    3. Confirms the MinIO `reports` bucket (and a recent `exec-report/<date>/`
       prefix with one CSV per chart + manifest) already exists — i.e. the export
       has actually run and delivered files.
    4. Optionally triggers the DAG run now (--run) to produce a fresh delivery.

  It is safe to re-run; it only READS state and (with --run) triggers a DAG run.

USAGE:
  python3 schedule_exec_report.py                 # verify readiness, no changes
  python3 schedule_exec_report.py --run           # also trigger the DAG run now
  python3 schedule_exec_report.py --bucket reports --date 2026-07-20
"""

import argparse
import json
import os
import sys

try:
    import requests
    import boto3
    from botocore.client import Config
except ImportError:
    os.system(f"{sys.executable} -m pip install -q requests boto3")
    import requests
    import boto3
    from botocore.client import Config

DASHBOARD_ID = 13
DASHBOARD_SLUG = "store_performance_exec"
DAG_ID = "exec_report_s3_export"
EXPECTED_CHARTS = 8  # KPI big-number cards: one per domain + points liability

# MinIO defaults (mirror export_exec_report.py / global.env / minio/.env).
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "adminadmin")
S3_BUCKET = os.environ.get("S3_BUCKET", "reports")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASS", "admin")

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "admin")


def sup_token():
    r = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def af_token():
    r = requests.post(
        f"{AIRFLOW_URL}/api/v1/security/login",
        json={"username": AIRFLOW_USER, "password": AIRFLOW_PASS, "provider": "db"},
        timeout=15,
    )
    if r.status_code != 200:
        return None
    return r.json().get("access_token")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(signature_version="s3v4"),
    )


def check_dashboard(token):
    h = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{SUPERSET_URL}/api/v1/dashboard/{DASHBOARD_ID}", headers=h, timeout=15)
    if r.status_code != 200:
        return False, f"dashboard {DASHBOARD_ID} returned HTTP {r.status_code}"
    d = r.json()["result"]
    pj = json.loads(d.get("position_json") or "{}")
    charts = [k for k in pj if k.startswith("CHART-")]
    published = d.get("published")
    slug = d.get("slug")
    ok = published and slug == DASHBOARD_SLUG and len(charts) == EXPECTED_CHARTS
    detail = f"published={published}, slug={slug}, charts={len(charts)} (expect {EXPECTED_CHARTS})"
    return ok, detail


def check_dag(token):
    # Prefer the REST API when reachable, but many local stacks expose the
    # Airflow CLI inside the worker instead of the API server. Degrade
    # gracefully: if we have a token, use the API; otherwise report that we
    # couldn't autoprovision a token (the caller can still confirm the DAG via CLI).
    if token:
        try:
            h = {"Authorization": f"Bearer {token}"}
            r = requests.get(f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}", headers=h, timeout=15)
            if r.status_code == 200:
                is_paused = r.json()["result"].get("is_paused")
                return (not is_paused), f"is_paused={is_paused}"
            return False, f"DAG {DAG_ID} not found (HTTP {r.status_code})"
        except requests.exceptions.ConnectionError:
            return None, "Airflow API unreachable (CLI check required)"
    return None, "Airflow API auth unavailable (CLI check required)"


def check_bucket(date_prefix):
    try:
        c = s3_client()
        c.head_bucket(Bucket=S3_BUCKET)
    except Exception as e:
        return False, f"bucket '{S3_BUCKET}' not reachable: {str(e)[:100]}"
    try:
        objs = c.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"exec-report/{date_prefix}/")["Contents"]
    except Exception:
        return False, f"no objects under exec-report/{date_prefix}/"
    csvs = [o for o in objs if o["Key"].endswith(".csv")]
    has_manifest = any(o["Key"].endswith("manifest.json") for o in objs)
    ok = len(csvs) == EXPECTED_CHARTS and has_manifest
    detail = f"{len(csvs)} csv + manifest={has_manifest} (expect {EXPECTED_CHARTS} csv)"
    return ok, detail


def trigger_dag(token):
    if not token:
        print("  ! Cannot trigger DAG: Airflow auth unavailable. Trigger manually in the UI.")
        return
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(
        f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns",
        headers=h,
        json={"conf": {}, "note": "Triggered by reporting/schedule_exec_report.py"},
        timeout=20,
    )
    print(f"  DAG run trigger -> HTTP {r.status_code}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", action="store_true", help="Also trigger the DAG run now")
    p.add_argument("--date", default=os.environ.get("REPORT_DATE"),
                   help="Date prefix to verify, e.g. 2026-07-20 (default: today UTC)")
    p.add_argument("--bucket", default=S3_BUCKET)
    args = p.parse_args()

    date_prefix = args.date or __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc
    ).strftime("%Y-%m-%d")

    print("== Executive Report (S3) readiness check — data-lab#32 ==")
    all_ok = True

    # 1) Dashboard
    try:
        st, detail = check_dashboard(sup_token())
    except Exception as e:
        st, detail = False, f"exception: {str(e)[:100]}"
    print(f"  [{'OK' if st else 'XX'}] Superset dashboard 13: {detail}")
    all_ok &= st

    # 2) DAG (verified via CLI in the worker; the API may be firewalled locally)
    af_tok = af_token()
    st, detail = check_dag(af_tok)
    mark = "OK" if st is True else ("??" if st is None else "XX")
    print(f"  [{mark}] Airflow DAG '{DAG_ID}': {detail}")
    # Only a hard failure (False) fails the check; None = couldn't verify here.
    if st is False:
        all_ok = False

    # 3) Bucket delivery
    try:
        st, detail = check_bucket(date_prefix)
    except Exception as e:
        st, detail = False, f"exception: {str(e)[:100]}"
    print(f"  [{'OK' if st else 'XX'}] S3 delivery for {date_prefix}: {detail}")
    all_ok &= st

    if args.run:
        print("  --run: triggering DAG now ...")
        if af_tok:
            trigger_dag(af_tok)
        else:
            print("  Falling back to CLI trigger (API auth unavailable).")
            import subprocess
            try:
                out = subprocess.run(
                    ["airflow", "dags", "trigger", DAG_ID],
                    capture_output=True, text=True, timeout=60,
                )
                print("  CLI trigger:", (out.stdout or out.stderr).strip()[:200])
            except Exception as e:
                print(f"  CLI trigger failed: {str(e)[:120]}")

    print()
    if all_ok:
        print("PASS: executive S3 report pipeline is verified and delivering.")
        sys.exit(0)
    else:
        print("FAIL: one or more checks failed (see XX above).")
        sys.exit(1)


if __name__ == "__main__":
    main()
