#!/usr/bin/env python3
"""
OpenMetadata setup — seeds service connections on first start.
Idempotent: safe to re-run; existing services are updated in place.

Services created:
  Database:   Gas Station EDW (postgres:5432/gas_station)
              Grocery EDW (postgres:5432/grocery)
              Verisim Gas Station Source (IP:5499/gas_station)
              Verisim Grocery Source (IP:5499/grocery)
  Pipeline:   Airflow (airflow-apiserver:8080)
  Dashboard:  Superset (superset:8088)
  dbt:        Ingestion pipelines linked to EDW for gas station + grocery
              (if manifest.json exists for each project)
"""

import base64
import json
import os
import sys
import time

import requests

BASE_URL = "http://openmetadata-server:8585/api/v1"
ADMIN_EMAIL    = os.getenv("OPENMETADATA_ADMIN_EMAIL", "admin@open-metadata.org")
ADMIN_PASSWORD = os.getenv("OPENMETADATA_ADMIN_PASSWORD", "Admin1234@")

POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
IP                = os.getenv("IP", "127.0.0.1")
AIRFLOW_USER      = os.getenv("AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_PASS      = os.getenv("AIRFLOW_WWW_USER_PASSWORD", "airflow")
SUPERSET_PASS     = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
VERISIM_USER      = os.getenv("VERISIM_POSTGRES_USER", "verisim")
VERISIM_PASS      = os.getenv("VERISIM_POSTGRES_PASSWORD", "verisim")

DBT_GS_MANIFEST = "/dbt/gasstation/target/manifest.json"
DBT_GS_CATALOG  = "/dbt/gasstation/target/catalog.json"
DBT_GS_RESULTS  = "/dbt/gasstation/target/run_results.json"

DBT_GR_MANIFEST = "/dbt/grocery/target/manifest.json"
DBT_GR_CATALOG  = "/dbt/grocery/target/catalog.json"
DBT_GR_RESULTS  = "/dbt/grocery/target/run_results.json"


def wait_for_server(retries=40, delay=15):
    print("Waiting for OpenMetadata to be ready...")
    for i in range(retries):
        try:
            r = requests.get(f"{BASE_URL}/system/status", timeout=5)
            if r.status_code in (200, 401):
                print("OpenMetadata is ready.")
                return
        except Exception:
            pass
        print(f"  Not ready yet ({i+1}/{retries}), retrying in {delay}s...")
        time.sleep(delay)
    sys.exit("ERROR: OpenMetadata did not become ready in time.")


def login():
    encoded_password = base64.b64encode(ADMIN_PASSWORD.encode()).decode()
    r = requests.post(f"{BASE_URL}/users/login", json={
        "email": ADMIN_EMAIL,
        "password": encoded_password,
    }, timeout=10)
    r.raise_for_status()
    token = r.json()["accessToken"]
    print("Authenticated.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def upsert(headers, endpoint, payload, label):
    """PUT to create or update a resource. Returns response JSON or None."""
    r = requests.put(f"{BASE_URL}{endpoint}", headers=headers,
                     data=json.dumps(payload), timeout=15)
    if r.status_code in (200, 201):
        print(f"  OK: {label}")
        return r.json()
    else:
        print(f"  WARN: {label} — {r.status_code}: {r.text[:300]}")
        return None


def register_dbt_pipeline(headers, service_result, pipeline_name, display_name,
                           manifest, catalog, results):
    """Register a dbt ingestion pipeline if the manifest exists."""
    if not os.path.exists(manifest):
        print(f"  SKIP: manifest.json not found at {manifest}")
        print("  Run the Airflow pipeline DAG first, then re-run this setup.")
        return
    if service_result is None:
        print("  SKIP: EDW service was not created — cannot link dbt pipeline.")
        return

    config = {"type": "DBTLocalConfig", "dbtManifestFilePath": manifest}
    if os.path.exists(catalog):
        config["dbtCatalogFilePath"] = catalog
    if os.path.exists(results):
        config["dbtRunResultsFilePath"] = results

    upsert(headers, "/services/ingestionPipelines", {
        "name": pipeline_name,
        "displayName": display_name,
        "pipelineType": "dbt",
        "service": {"id": service_result.get("id"), "type": "databaseService"},
        "sourceConfig": {"config": config},
        "airflowConfig": {"scheduleInterval": "*/15 * * * *"},
    }, f"{display_name}")


def main():
    wait_for_server()
    time.sleep(5)  # brief buffer after health check passes
    headers = login()

    # -------------------------------------------------------------------------
    # 1. Gas Station EDW
    # -------------------------------------------------------------------------
    print("\n[1/7] Gas Station EDW database service")
    gs_edw = upsert(headers, "/services/databaseServices", {
        "name": "GasStation",
        "displayName": "Gas Station \u2014 EDW",
        "serviceType": "Postgres",
        "connection": {
            "config": {
                "type": "Postgres",
                "scheme": "postgresql+psycopg2",
                "username": POSTGRES_USER,
                "authType": {"password": POSTGRES_PASSWORD},
                "hostPort": "postgres:5432",
                "database": "gas_station",
            }
        },
    }, "Gas Station EDW (postgres:5432/gas_station)")

    # -------------------------------------------------------------------------
    # 2. Grocery EDW
    # -------------------------------------------------------------------------
    print("\n[2/7] Grocery EDW database service")
    gr_edw = upsert(headers, "/services/databaseServices", {
        "name": "Grocery",
        "displayName": "Grocery \u2014 EDW",
        "serviceType": "Postgres",
        "connection": {
            "config": {
                "type": "Postgres",
                "scheme": "postgresql+psycopg2",
                "username": POSTGRES_USER,
                "authType": {"password": POSTGRES_PASSWORD},
                "hostPort": "postgres:5432",
                "database": "grocery",
            }
        },
    }, "Grocery EDW (postgres:5432/grocery)")

    # -------------------------------------------------------------------------
    # 3. Verisim — Gas Station Source
    # -------------------------------------------------------------------------
    print("\n[3/7] Verisim Gas Station source database service")
    upsert(headers, "/services/databaseServices", {
        "name": "VerisimGasStation",
        "displayName": "Verisim \u2014 Gas Station Source",
        "serviceType": "Postgres",
        "connection": {
            "config": {
                "type": "Postgres",
                "scheme": "postgresql+psycopg2",
                "username": VERISIM_USER,
                "authType": {"password": VERISIM_PASS},
                "hostPort": f"{IP}:5499",
                "database": "gas_station",
            }
        },
    }, f"Verisim Gas Station ({IP}:5499/gas_station)")

    # -------------------------------------------------------------------------
    # 4. Verisim — Grocery Source
    # -------------------------------------------------------------------------
    print("\n[4/7] Verisim Grocery source database service")
    upsert(headers, "/services/databaseServices", {
        "name": "VerisimGrocery",
        "displayName": "Verisim \u2014 Grocery Source",
        "serviceType": "Postgres",
        "connection": {
            "config": {
                "type": "Postgres",
                "scheme": "postgresql+psycopg2",
                "username": VERISIM_USER,
                "authType": {"password": VERISIM_PASS},
                "hostPort": f"{IP}:5499",
                "database": "grocery",
            }
        },
    }, f"Verisim Grocery ({IP}:5499/grocery)")

    # -------------------------------------------------------------------------
    # 5. Airflow — Pipeline Service
    # -------------------------------------------------------------------------
    print("\n[5/7] Airflow pipeline service")
    upsert(headers, "/services/pipelineServices", {
        "name": "Airflow",
        "displayName": "Airflow",
        "serviceType": "Airflow",
        "connection": {
            "config": {
                "type": "Airflow",
                "hostPort": "http://airflow-apiserver:8080",
            }
        },
    }, "Airflow (airflow-apiserver:8080)")

    # -------------------------------------------------------------------------
    # 6. Superset — Dashboard Service
    # -------------------------------------------------------------------------
    print("\n[6/7] Superset dashboard service")
    upsert(headers, "/services/dashboardServices", {
        "name": "Superset",
        "displayName": "Superset \u2014 BI",
        "serviceType": "Superset",
        "connection": {
            "config": {
                "type": "Superset",
                "hostPort": "http://superset:8088",
            }
        },
    }, "Superset (superset:8088)")

    # -------------------------------------------------------------------------
    # 7. dbt ingestion pipelines
    # -------------------------------------------------------------------------
    print("\n[7/7] dbt ingestion pipelines")
    print("  Gas Station:")
    register_dbt_pipeline(
        headers, gs_edw,
        "gas-station-dbt-metadata", "Gas Station dbt Metadata",
        DBT_GS_MANIFEST, DBT_GS_CATALOG, DBT_GS_RESULTS,
    )
    print("  Grocery:")
    register_dbt_pipeline(
        headers, gr_edw,
        "grocery-dbt-metadata", "Grocery dbt Metadata",
        DBT_GR_MANIFEST, DBT_GR_CATALOG, DBT_GR_RESULTS,
    )

    print("\nSetup complete.")
    print(f"  OpenMetadata UI: http://openmetadata-server:8585")
    print(f"  Login: {ADMIN_EMAIL} / {ADMIN_PASSWORD}")


if __name__ == "__main__":
    main()
