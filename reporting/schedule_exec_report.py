#!/usr/bin/env python3
"""
Create (idempotently) the Store Performance Executive daily digest report
schedule in Superset (data-lab#32).

This wires the Executive Overview dashboard (id 13, slug `store_performance_exec`)
to a daily email snapshot. Run it once Superset's report worker is available
(see PREREQUISITES below). It is safe to re-run: it skips if a schedule with
the same name already exists.

PREREQUISITES (Infra Dev — see data-lab#35 for enabling scheduled reports):
  1. `ALERT_REPORTS` feature flag enabled in superset_config.py FEATURE_FLAGS.
  2. A Superset celery worker + celery beat present in the stack (the report
     scheduler runs as a celery task; the Airflow worker does NOT serve it).
  3. SMTP configured (SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASSWORD /
     SMTP_MAIL_FROM) so the email can actually be delivered.

Until those exist, the /api/v1/report/ blueprint is not registered and this
script will report a clear blocker instead of failing silently.

REPORT FORMAT NOTE: the stock apache/superset:4.1.2 image ships no headless
browser, so PNG/PDF *screenshot* reports cannot render until the worker image
gains Chromium. CSV (data attachment) and LINK (URL in email) reports work on
the stock image. Default below is CSV for that reason; pass --format PNG/PDF
only once a Chromium-enabled worker exists (Infra Dev ticket data-lab#35).

Usage:
  python3 schedule_exec_report.py \
      --superset-url http://localhost:8088 \
      --username admin --password admin \
      --recipient chris@example.com \
      --crontab "0 7 * * *" \
      --timezone "America/New_York"
"""

import argparse
import json
import os
import sys

try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

DASHBOARD_SLUG = "store_performance_exec"
SCHEDULE_NAME = "Store Performance — Executive Daily Digest"


def get_token(url, username, password):
    r = requests.post(
        f"{url}/api/v1/security/login",
        json={"username": username, "password": password, "provider": "db"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def h(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def find_dashboard_id(token, base_url, slug):
    r = requests.get(
        f"{base_url}/api/v1/dashboard/",
        headers=h(token),
        params={"q": f"(page:0,page_size:50,filters:!((col:slug,opr:eq,value:{slug})))"},
        timeout=10,
    )
    if r.status_code != 200:
        return None
    for d in r.json().get("result", []):
        if d.get("slug") == slug:
            return d["id"]
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--superset-url", default=os.environ.get("SUPERSET_URL", "http://localhost:8088"))
    p.add_argument("--username", default="admin")
    p.add_argument("--password", default="admin")
    p.add_argument("--recipient", default=os.environ.get("REPORT_RECIPIENT_EMAIL", ""))
    p.add_argument("--crontab", default="0 7 * * *")
    p.add_argument("--timezone", default="America/New_York")
    p.add_argument(
        "--format",
        dest="report_format",
        default="CSV",
        choices=["CSV", "PNG", "PDF"],
        help="CSV/LINK work on stock image (no headless browser). PNG/PDF need a Chromium-enabled worker (data-lab#35).",
    )
    args = p.parse_args()

    if not args.recipient:
        print("ERROR: --recipient (or REPORT_RECIPIENT_EMAIL) is required for email delivery.")
        sys.exit(2)

    token = get_token(args.superset_url, args.username, args.password)

    # Probe the report blueprint. If ALERT_REPORTS is off it returns 404.
    probe = requests.get(f"{args.superset_url}/api/v1/report/", headers=h(token), timeout=10)
    if probe.status_code == 404:
        print(
            "BLOCKED: /api/v1/report/ is not registered.\n"
            "The ALERT_REPORTS feature flag is off (or no celery report worker is present).\n"
            "Enable scheduled reports per data-lab#35 (Infra Dev), then re-run this script."
        )
        sys.exit(3)
    probe.raise_for_status()

    dash_id = find_dashboard_id(token, args.superset_url, DASHBOARD_SLUG)
    if not dash_id:
        print(f"ERROR: dashboard slug '{DASHBOARD_SLUG}' not found.")
        sys.exit(4)

    # Idempotency: skip if a schedule with this name already exists.
    existing = requests.get(
        f"{args.superset_url}/api/v1/report/",
        headers=h(token),
        params={"q": f"(page:0,page_size:50,filters:!((col:name,opr:eq,value:{SCHEDULE_NAME})))"},
        timeout=10,
    )
    for s in existing.json().get("result", []):
        if s.get("name") == SCHEDULE_NAME:
            print(f"~ Report schedule '{SCHEDULE_NAME}' already exists (id={s['id']}). Nothing to do.")
            return

    payload = {
        "report_schedule": {
            "type": "ReportScheduleType.DAILY",
            "name": SCHEDULE_NAME,
            "crontab": args.crontab,
            "timezone": args.timezone,
            "active": True,
            "dashboard_id": dash_id,
            "report_format": args.report_format,
            "description": "Daily executive digest of top-line KPIs across all six store domains.",
            "recipients": [
                {
                    "recipient_type": "Email",
                    "recipient_config_json": {"target": args.recipient},
                }
            ],
            "log_retention": 90,
            "grace_period": 14400,
            "working_timeout": 3600,
        }
    }
    r = requests.post(
        f"{args.superset_url}/api/v1/report/",
        headers=h(token),
        json=payload,
        timeout=30,
    )
    if r.status_code in (200, 201):
        print(f"✓ Created report schedule '{SCHEDULE_NAME}' (id={r.json().get('id')})")
    else:
        print(f"✗ Failed to create report schedule: {r.status_code} {r.text[:300]}")
        sys.exit(5)


if __name__ == "__main__":
    main()
