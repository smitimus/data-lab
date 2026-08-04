#!/usr/bin/env python3
"""
Create Data Quality dashboard in Superset via REST API.

Registers new mart datasets (transport, timeclock, fulfillment) and builds
a Data Quality & Operations monitoring dashboard.

Usage:
  python3 create_data_quality_dashboard.py [--superset-url URL] [--username USER] [--password PASS]
"""

import json
import sys
import os
import random
import string
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

SUPERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin"


def find_grocery_db_id(token, base_url):
    """Find the Grocery database ID by name."""
    r = requests.get(f"{base_url}/api/v1/database/", headers=headers(token))
    r.raise_for_status()
    for db in r.json().get("result", []):
        if db["database_name"] == "Grocery":
            return db["id"]
    raise RuntimeError("Grocery database not found in Superset")


def get_token(url, username, password):
    resp = requests.post(urljoin(url, "/api/v1/security/login"),
        json={"username": username, "password": password, "provider": "db"}, timeout=10)
    resp.raise_for_status()
    return resp.json()["access_token"]


def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def make_metric(col, agg="SUM", label=None):
    # Clean snake_case label so the SQL alias survives pandas postprocessing.
    clean = label or f"{agg.lower()}_{col}"
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col, "type": ""},
        "aggregate": agg,
        "label": clean,
    }


def make_filter(col, op, val, clause="WHERE"):
    return {
        # New API format
        "col": col,
        "op": op,
        "comparator": val,
        # Old explore_json format
        "expressionType": "SIMPLE",
        "subject": col,
        "operator": op,
        "clause": clause,
    }


def register_dataset(token, base_url, db_id, table_name, schema):
    """Register an existing table as a Superset dataset."""
    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table_name,
        "owners": [1],
    }
    resp = requests.post(urljoin(base_url, "/api/v1/dataset/"),
        headers=headers(token), json=payload, timeout=10)
    if resp.status_code in (200, 201):
        ds_id = resp.json().get("id")
        print(f"  ✓ Registered '{schema}.{table_name}' (id={ds_id})")
        return ds_id
    elif resp.status_code == 422:
        print(f"  ~ '{schema}.{table_name}' may already exist")
        return None
    else:
        print(f"  ✗ Failed '{schema}.{table_name}': {resp.status_code} {resp.text[:100]}")
        return None


def set_main_dttm(token, base_url, ds_id, col):
    """Set the main datetime column on a dataset."""
    resp = requests.put(urljoin(base_url, f"/api/v1/dataset/{ds_id}"),
        headers=headers(token), json={"main_dttm_col": col}, timeout=10)
    if resp.status_code == 200:
        print(f"  ✓ Set main_dttm_col='{col}' on dataset {ds_id}")
    else:
        print(f"  ✗ Failed: {resp.status_code} {resp.text[:100]}")


def create_chart(token, base_url, ds_id, slice_name, viz_type, params_extra):
    """Create a Superset chart via POST /api/v1/chart/."""
    from _superset_query_context import build_query_context
    base_params = {
        "datasource": f"{ds_id}__table",
        "viz_type": viz_type,
        "time_range": "No filter",
        "datasource_type": "table",
        "adhoc_filters": [],
    }
    base_params.update(params_extra)
    # Charts on a temporal dataset need granularity_sqla in params (used by the
    # legacy /superset/explore_json/ endpoint). Auto-resolve from main_dttm_col.
    # Only inject for TIME-SERIES viz types; non-time-series charts (dist_bar,
    # pie, big_number, table, ...) must NOT carry a temporal column or the
    # legacy endpoint applies a broken rolling window ("Applied rolling window
    # did not return any data").
    NON_TIME_VIZ = {
        "dist_bar", "pie", "big_number", "big_number_total", "table",
        "word_cloud", "treemap", "sunburst", "sankey", "chord", "world_map",
        "histogram", "box_plot", "heatmap", "rose", "funnel", "gauge",
        "graph_chart", "mapbox", "deck_scatter", "deck_sandwich", "deck_path",
        "deck_arc", "deck_grid", "deck_hex", "deck_geojson", "deck_polygon",
        "paired_ttest", "rooted_ttest", "filter_box",
    }
    if (not base_params.get("granularity_sqla") and not base_params.get("granularity")
            and token and viz_type not in NON_TIME_VIZ):
        try:
            r = requests.get(urljoin(base_url, f"/api/v1/dataset/{ds_id}"),
                              headers=headers(token), timeout=10)
            if r.status_code == 200:
                dttm = r.json().get("result", {}).get("main_dttm_col")
                if dttm:
                    base_params["granularity_sqla"] = dttm
        except Exception:
            pass
    # Fix: pie and big_number charts expect singular "metric", not "metrics"
    if viz_type in ("pie", "big_number_total", "big_number"):
        if "metrics" in base_params and "metric" not in base_params:
            val = base_params.pop("metrics")
            if isinstance(val, list) and len(val) > 0:
                base_params["metric"] = val[0]
            elif isinstance(val, dict):
                base_params["metric"] = val
    payload = {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(base_params),
        "query_context": build_query_context(ds_id, base_params, token, base_url),
        "dashboards": [],
    }
    resp = requests.post(urljoin(base_url, "/api/v1/chart/"),
        headers=headers(token), json=payload, timeout=30)
    if resp.status_code == 201:
        result = resp.json()
        cid = result["id"]
        print(f"  ✓ Chart '{slice_name}' (id={cid})")
        return {"id": cid, "slice_name": slice_name}
    else:
        print(f"  ✗ Failed '{slice_name}': {resp.status_code} {resp.text[:200]}")
        return None


def _link_charts(token, base_url, dash_id, chart_ids):
    """Link charts to dashboard via the chart's dashboards relationship.

    Without this the frontend shows 'no chart definition associated with this
    component' because the chart metadata isn't hydrated into the dashboard.
    """
    for c in chart_ids:
        cid = c["id"] if isinstance(c, dict) else c
        try:
            resp = requests.get(urljoin(base_url, f"/api/v1/chart/{cid}"),
                                headers=headers(token), timeout=10)
            if resp.status_code == 200:
                existing = resp.json().get("result", {}).get("dashboards", [])
                dash_ids = [d["id"] for d in existing if isinstance(d, dict)]
                if dash_id not in dash_ids:
                    dash_ids.append(dash_id)
                requests.put(urljoin(base_url, f"/api/v1/chart/{cid}"),
                             headers=headers(token),
                             json={"dashboards": dash_ids}, timeout=10)
        except Exception as e:
            print(f"  ⚠ Failed to link chart {cid} to dashboard {dash_id}: {e}")


def create_dashboard(token, base_url, chart_ids, title, slug):
    """Create dashboard with charts arranged in rows of 3."""
    def rand_id(prefix="R", n=4):
        return prefix + "".join(random.choices(string.ascii_uppercase + string.digits, k=n))

    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }

    row_idx = 0
    for i in range(0, len(chart_ids), 3):
        row_charts = chart_ids[i:i+3]
        row_id = f"ROW-{row_idx}"
        row_children = []
        for c in row_charts:
            ckey = f"CHART-{c['id']}"
            position[ckey] = {
                "type": "CHART", "id": ckey, "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": c["id"], "width": 4, "height": 60, "sliceName": c["slice_name"]},
            }
            row_children.append(ckey)
        position[row_id] = {
            "type": "ROW", "id": row_id, "children": row_children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        position["GRID_ID"]["children"].append(row_id)
        row_idx += 1

    payload = {
        "dashboard_title": title,
        "slug": slug,
        "published": True,
        "position_json": json.dumps(position),
        "json_metadata": json.dumps({
            "chart_configuration": {},
            "global_chart_configuration": {
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": [c["id"] for c in chart_ids],
            },
            "refresh_frequency": 0,
            "color_scheme": "",
            "label_colors": {},
            "cross_filters_enabled": True,
        }),
    }

    resp = requests.post(urljoin(base_url, "/api/v1/dashboard/"),
        headers=headers(token), json=payload, timeout=30)
    if resp.status_code == 201:
        result = resp.json()
        print(f"  ✓ Dashboard '{title}' (ID={result['id']})")
        _link_charts(token, base_url, result["id"], chart_ids)
        return result
    elif resp.status_code == 422:
        # Dashboard may exist — try updating
        list_r = requests.get(urljoin(base_url, "/api/v1/dashboard/"),
            headers=headers(token),
            params={"q": f'(page:0,page_size:50,filters:!((col:slug,opr:eq,value:{slug})))'},
            timeout=10)
        if list_r.status_code == 200:
            for d in list_r.json().get("result", []):
                if d.get("slug") == slug:
                    print(f"  ℹ Dashboard exists (ID={d['id']}) — updating")
                    resp2 = requests.put(urljoin(base_url, f"/api/v1/dashboard/{d['id']}"),
                        headers=headers(token), json=payload, timeout=30)
                    if resp2.status_code == 200:
                        print(f"  ✓ Updated (ID={d['id']})")
                        _link_charts(token, base_url, d["id"], chart_ids)
                        return resp2.json()
                    print(f"  ✗ Update failed: {resp2.status_code} {resp2.text[:100]}")
                    return None
        print(f"  ✗ Dashboard create failed: {resp.status_code} {resp.text[:200]}")
        return None
    else:
        print(f"  ✗ Dashboard create failed: {resp.status_code} {resp.text[:200]}")
        return None


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--superset-url", default=SUPERSET_URL)
    parser.add_argument("--username", default=USERNAME)
    parser.add_argument("--password", default=PASSWORD)
    args = parser.parse_args()

    print("=== Data Quality Dashboard Creator ===\n")
    token = get_token(args.superset_url, args.username, args.password)
    print("✓ Authenticated\n")

    # Discover grocery DB ID
    grocery_db_id = find_grocery_db_id(token, args.superset_url)
    print(f"Grocery DB ID: {grocery_db_id}\n")

    # Step 1: Register new mart datasets
    print("--- Step 1: Register New Mart Datasets ---")
    new_marts = [
        ("mart_transport_load_summary", "mart"),
        ("mart_fleet_utilization", "mart"),
        ("mart_transport_daily_metrics", "mart"),
        ("mart_attendance_summary", "mart"),
        ("mart_daily_attendance_stats", "mart"),
        ("mart_employee_hours_vs_schedule", "mart"),
        ("mart_fulfillment_operations", "mart"),
        ("mart_fulfillment_pick_accuracy", "mart"),
        ("mart_daily_fulfillment_summary", "mart"),
        ("mart_order_fulfillment_funnel", "mart"),
    ]
    ds = {}
    for table, schema in new_marts:
        ds_id = register_dataset(token, args.superset_url, grocery_db_id, table, schema)
        if ds_id:
            ds[table] = ds_id

    # Fetch all existing dataset IDs for the new marts
    list_resp = requests.get(urljoin(args.superset_url, "/api/v1/dataset/"),
        headers=headers(token), params={"page": 0, "page_size": 50}, timeout=10)
    for d in list_resp.json().get("result", []):
        tbl = d.get("table_name", "")
        if tbl.startswith("mart_") and d["id"] > 18:
            ds[tbl] = d["id"]
            print(f"  ✓ Found existing dataset '{tbl}' (id={d['id']})")

    # Map dataset keys (existing + new)
    KNOWN = {
        "mart_daily_revenue": 1,
        "mart_product_performance": 2,
        "mart_department_performance": 3,
        "mart_location_performance": 4,
        "mart_inventory_summary": 5,
        "mart_inventory_turnover": 7,
        "mart_hourly_sales_pattern": 8,
        "mart_delivery_performance": 9,
        "mart_store_weekly_summary": 10,
    }
    KNOWN.update(ds)
    print()

    # Step 2: Set date columns on new datasets
    print("--- Step 2: Configure Date Columns ---")
    for table, col in [
        ("mart_transport_load_summary", "load_date"),
        ("mart_transport_daily_metrics", "load_date"),
        ("mart_attendance_summary", "event_date"),
        ("mart_daily_attendance_stats", "event_date"),
        ("mart_employee_hours_vs_schedule", "report_date"),
        ("mart_fulfillment_operations", "order_received_at"),
        ("mart_daily_fulfillment_summary", "report_date"),
        ("mart_order_fulfillment_funnel", "order_date"),
    ]:
        if table in ds and ds[table]:
            set_main_dttm(token, args.superset_url, ds[table], col)
    print()

    # Step 3: Create charts
    print("--- Step 3: Create Charts ---")
    charlist = []
    
    sections = [
        # ── Row 1: Data Overview ──
        {
            "key": "mart_inventory_turnover",
            "name": "Stock Aging",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("quantity_on_hand", "SUM")],
                "groupby": ["stock_aging_category"],
                "row_limit": 10,
            }
        },
        {
            "key": "mart_inventory_turnover",
            "name": "Inventory Location Breakdown",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("quantity_on_hand", "SUM")],
                "groupby": ["location_name"],
                "row_limit": 10,
            }
        },
        {
            "key": "mart_hourly_sales_pattern",
            "name": "Avg Transaction Count by Hour",
            "viz": "line",
            "params": {
                "metrics": [make_metric("transaction_count", "AVG")],
                "groupby": ["hour_of_day"],
                "row_limit": 24,
            }
        },
        # ── Row 2: Transport ──
        {
            "key": "mart_fleet_utilization",
            "name": "Fleet Loads per Truck",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_loads", "SUM")],
                "groupby": ["license_plate"],
                "row_limit": 10,
            }
        },
        {
            "key": "mart_transport_daily_metrics",
            "name": "Daily Completion Rate",
            "viz": "line",
            "params": {
                "metrics": [make_metric("completion_rate_pct", "AVG")],
                "groupby": ["warehouse_name"],
                "row_limit": 365,
            }
        },
        {
            "key": "mart_transport_load_summary",
            "name": "Load Status Breakdown",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("load_id", "COUNT_DISTINCT")],
                "groupby": ["status"],
                "row_limit": 10,
            }
        },
        # ── Row 3: Attendance ──
        {
            "key": "mart_daily_attendance_stats",
            "name": "Daily Employees Present",
            "viz": "line",
            "params": {
                "metrics": [make_metric("employees_present", "AVG")],
                "groupby": ["location_name"],
                "row_limit": 365,
            }
        },
        {
            "key": "mart_employee_hours_vs_schedule",
            "name": "Attendance Status",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("employee_id", "COUNT_DISTINCT")],
                "groupby": ["attendance_status"],
                "row_limit": 10,
            }
        },
        {
            "key": "mart_attendance_summary",
            "name": "Avg Net Hours Worked",
            "viz": "line",
            "params": {
                "metrics": [make_metric("net_hours_worked", "AVG")],
                "row_limit": 365,
            }
        },
        # ── Row 4: Fulfillment ──
        {
            "key": "mart_daily_fulfillment_summary",
            "name": "Daily Fill Rate",
            "viz": "line",
            "params": {
                "metrics": [make_metric("daily_fill_rate_pct", "AVG")],
                "groupby": ["warehouse_name"],
                "row_limit": 365,
            }
        },
        {
            "key": "mart_fulfillment_pick_accuracy",
            "name": "Perfect Order Rate",
            "viz": "big_number_total",
            "params": {
                "metric": make_metric("is_perfect_order", "COUNT", label="Perfect Orders"),
                "adhoc_filters": [make_filter("is_perfect_order", "==", True)],
                "subheader": "Fully Picked Orders",
            }
        },
        {
            "key": "mart_order_fulfillment_funnel",
            "name": "Pipeline Stage Distribution",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("order_id", "COUNT_DISTINCT")],
                "groupby": ["pipeline_stage"],
                "row_limit": 10,
            }
        },
    ]

    for sec in sections:
        ds_id = KNOWN.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                charlist.append(c)
    print()

    # Step 4: Create dashboard
    print("--- Step 4: Create Dashboard ---")
    if charlist:
        create_dashboard(token, args.superset_url, charlist,
            "Data Quality & Operations", "data-quality-ops")
    else:
        print("  ✗ No charts created")
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
