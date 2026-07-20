#!/usr/bin/env python3
"""
Create missing Superset dashboards for HR & Labor, Shrink & Loss, Fulfillment,
Supply Chain, and Store (per-domain) dashboards.

Registers unused mart tables, builds charts, and creates dedicated dashboards
instead of cramming everything into the Data Quality catch-all.

Usage:
  python3 create_missing_dashboards.py [--superset-url URL] [--username USER] [--password PASS]
"""

import json
import sys
import os
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
    resp = requests.post(
        urljoin(url, "/api/v1/security/login"),
        json={"username": username, "password": password, "provider": "db"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def make_metric(col, agg="SUM", label=None):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col, "type": ""},
        "aggregate": agg,
        "label": label or f"{agg}({col})",
    }


def register_dataset(token, base_url, db_id, table_name, schema="mart"):
    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table_name,
        "owners": [1],
    }
    resp = requests.post(
        urljoin(base_url, "/api/v1/dataset/"),
        headers=headers(token),
        json=payload,
        timeout=10,
    )
    if resp.status_code in (200, 201):
        ds_id = resp.json().get("id")
        print(f"  ✓ Registered '{schema}.{table_name}' (id={ds_id})")
        return ds_id
    elif resp.status_code == 422:
        # Already exists — recover the existing dataset id so callers can build charts on it.
        ds_id = _find_dataset_id(token, base_url, db_id, table_name, schema)
        if ds_id:
            print(f"  ~ '{schema}.{table_name}' already exists (id={ds_id})")
            return ds_id
        print(f"  ~ '{schema}.{table_name}' may already exist")
        return None
    else:
        print(f"  ✗ Failed '{schema}.{table_name}': {resp.status_code} {resp.text[:100]}")
        return None


def set_main_dttm(token, base_url, ds_id, col):
    resp = requests.put(
        urljoin(base_url, f"/api/v1/dataset/{ds_id}"),
        headers=headers(token),
        json={"main_dttm_col": col},
        timeout=10,
    )
    if resp.status_code == 200:
        print(f"  ✓ Set main_dttm_col='{col}' on dataset {ds_id}")
        return True
    else:
        print(f"  ✗ Failed: {resp.status_code} {resp.text[:100]}")
        return False


def create_chart(token, base_url, ds_id, slice_name, viz_type, params_extra):
    # Idempotent: reuse an existing chart with the same name (avoids duplicates on re-run).
    existing = _find_chart_id(token, base_url, slice_name)
    if existing:
        print(f"  ~ Chart '{slice_name}' already exists (id={existing})")
        return {"id": existing, "slice_name": slice_name}
    base_params = {
        "datasource": f"{ds_id}__table",
        "viz_type": viz_type,
        "time_range": "No filter",
        "datasource_type": "table",
        "adhoc_filters": [],
    }
    base_params.update(params_extra)
    payload = {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(base_params),
        "dashboards": [],
    }
    resp = requests.post(
        urljoin(base_url, "/api/v1/chart/"),
        headers=headers(token),
        json=payload,
        timeout=30,
    )
    if resp.status_code == 201:
        result = resp.json()
        print(f"  ✓ Chart '{slice_name}' (id={result['id']})")
        return {"id": result["id"], "slice_name": slice_name}
    else:
        print(f"  ✗ Failed '{slice_name}': {resp.status_code} {resp.text[:200]}")
        return None


def _find_dataset_id(token, base_url, db_id, table_name, schema="mart"):
    try:
        r = requests.get(
            urljoin(base_url, "/api/v1/dataset/"),
            headers=headers(token),
            params={"q": json.dumps({"page_size": 500})},
            timeout=20,
        )
        if r.status_code == 200:
            for d in r.json().get("result", []):
                if d.get("table_name") == table_name and d.get("schema") == schema:
                    return d["id"]
    except Exception:
        pass
    return None


def _find_chart_id(token, base_url, slice_name):
    try:
        r = requests.get(
            urljoin(base_url, "/api/v1/chart/"),
            headers=headers(token),
            params={"q": json.dumps({"page_size": 500})},
            timeout=20,
        )
        if r.status_code == 200:
            for c in r.json().get("result", []):
                if c.get("slice_name") == slice_name:
                    return c["id"]
    except Exception:
        pass
    return None


def create_dashboard(token, base_url, chart_ids, title, slug):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    row_idx = 0
    for i in range(0, len(chart_ids), 3):
        row_charts = chart_ids[i : i + 3]
        row_id = f"ROW-{row_idx}"
        row_children = []
        for c in row_charts:
            ckey = f"CHART-{c['id']}"
            position[ckey] = {
                "type": "CHART",
                "id": ckey,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": c["id"], "width": 4, "height": 60, "sliceName": c["slice_name"]},
            }
            row_children.append(ckey)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": row_children,
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
        "json_metadata": json.dumps(
            {
                "chart_configuration": {},
                "global_chart_configuration": {
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "chartsInScope": [c["id"] for c in chart_ids],
                },
                "refresh_frequency": 0,
                "color_scheme": "",
                "label_colors": {},
                "cross_filters_enabled": True,
            }
        ),
    }

    # Check if dashboard with this slug already exists
    list_r = requests.get(
        urljoin(base_url, "/api/v1/dashboard/"),
        headers=headers(token),
        params={"q": f'(page:0,page_size:50,filters:!((col:slug,opr:eq,value:{slug})))'},
        timeout=10,
    )
    if list_r.status_code == 200:
        for d in list_r.json().get("result", []):
            if d.get("slug") == slug:
                print(f"  ℹ Dashboard '{title}' exists (ID={d['id']}) — updating")
                resp2 = requests.put(
                    urljoin(base_url, f"/api/v1/dashboard/{d['id']}"),
                    headers=headers(token),
                    json=payload,
                    timeout=30,
                )
                if resp2.status_code == 200:
                    print(f"  ✓ Updated (ID={d['id']})")
                    # Re-link charts
                    _link_charts(token, base_url, d["id"], chart_ids)
                    return resp2.json()
                print(f"  ✗ Update failed: {resp2.status_code}")
                return None

    # Create new dashboard
    resp = requests.post(
        urljoin(base_url, "/api/v1/dashboard/"),
        headers=headers(token),
        json=payload,
        timeout=30,
    )
    if resp.status_code == 201:
        result = resp.json()
        print(f"  ✓ Dashboard '{title}' (ID={result['id']})")
        _link_charts(token, base_url, result["id"], chart_ids)
        return result
    else:
        print(f"  ✗ Dashboard create failed: {resp.status_code} {resp.text[:200]}")
        return None


def _link_charts(token, base_url, dash_id, chart_ids):
    """Link charts to dashboard via dashboard_slices through the API."""
    for c in chart_ids:
        try:
            # Get current chart's dashboards
            resp = requests.get(
                urljoin(base_url, f"/api/v1/chart/{c['id']}"),
                headers=headers(token),
                timeout=10,
            )
            if resp.status_code == 200:
                chart_data = resp.json()
                existing = chart_data.get("result", {}).get("dashboards", [])
                dash_ids = [d["id"] for d in existing if isinstance(d, dict)]
                if dash_id not in dash_ids:
                    dash_ids.append(dash_id)
                update_resp = requests.put(
                    urljoin(base_url, f"/api/v1/chart/{c['id']}"),
                    headers=headers(token),
                    json={"dashboards": dash_ids},
                    timeout=10,
                )
        except Exception as e:
            print(f"  ⚠ Failed to link chart {c['id']} to dashboard {dash_id}: {e}")


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--superset-url", default=SUPERSET_URL)
    parser.add_argument("--username", default=USERNAME)
    parser.add_argument("--password", default=PASSWORD)
    args = parser.parse_args()

    print("=== Missing Dashboards Creator ===\n")
    token = get_token(args.superset_url, args.username, args.password)
    print("✓ Authenticated\n")

    # Discover grocery DB ID
    grocery_db_id = find_grocery_db_id(token, args.superset_url)
    print(f"Grocery DB ID: {grocery_db_id}\n")

    # ── Step 1: Register unused datasets ─────────────────────────────────────
    print("--- Step 1: Register Datasets ---")
    new_tables = [
        "mart_department_labor",
        "mart_labor_efficiency",
        "mart_employee_productivity",
        "mart_employee_cost",
        "mart_loyalty_cohort",
        "mart_shrink_analysis",
        "mart_department_shrinkage",
        "mart_promotion_effectiveness",
        "mart_supply_chain_summary",
        # data-lab#26: POS & Sales + Promotions domain
        "mart_category_sales_performance",
        "mart_product_price_elasticity",
        "mart_promotion_redemption",
        "mart_promotion_effectiveness",
        # data-lab#27: Inventory & Shrinkage domain
        "mart_inventory_valuation",
        "mart_shrinkage_analysis",
        "mart_inventory_turnover",
        # data-lab#28: HR & Labor domain
        "mart_labor_cost_by_department",
        "mart_attendance_compliance",
        # data-lab#30: Transport & Logistics domain
        "mart_route_efficiency",
        "mart_fleet_cost",
        "mart_fleet_utilization",
        "mart_transport_daily_metrics",
    ]

    ds = {}
    for table in new_tables:
        ds_id = register_dataset(token, args.superset_url, grocery_db_id, table)
        if ds_id:
            ds[table] = ds_id

    # Also fetch existing IDs for tables we'll build charts on
    list_resp = requests.get(
        urljoin(args.superset_url, "/api/v1/dataset/"),
        headers=headers(token),
        params={"page": 0, "page_size": 100},
        timeout=10,
    )
    for d in list_resp.json().get("result", []):
        tbl = d.get("table_name", "")
        if tbl in new_tables and tbl not in ds:
            ds[tbl] = d["id"]
    print()

    # ── Step 2: Set date columns ─────────────────────────────────────────────
    print("--- Step 2: Configure Date Columns ---")
    date_map = {
        "mart_department_labor": "report_date",
        "mart_labor_efficiency": "scheduled_date",
        "mart_employee_productivity": "sale_date",
        "mart_employee_cost": "scheduled_date",
        "mart_loyalty_cohort": "signup_date",
        "mart_shrink_analysis": "event_date",
        "mart_department_shrinkage": "report_date",
        "mart_promotion_effectiveness": "ad_week_start",
        "mart_supply_chain_summary": "order_date",
        # data-lab#26: POS & Sales + Promotions domain
        "mart_category_sales_performance": "transaction_date",
        "mart_product_price_elasticity": "sale_month",
        "mart_promotion_redemption": "valid_from",
        "mart_promotion_effectiveness": "ad_week_start",
        # data-lab#27: Inventory & Shrinkage domain
        "mart_inventory_valuation": "location_id",
        "mart_shrinkage_analysis": "event_date",
        "mart_inventory_turnover": "last_updated",
        # data-lab#28: HR & Labor domain
        "mart_labor_cost_by_department": "report_date",
        "mart_attendance_compliance": "report_date",
        # data-lab#30: Transport & Logistics domain
        "mart_route_efficiency": "warehouse_name",
        "mart_fleet_cost": "license_plate",
        "mart_fleet_utilization": "license_plate",
        "mart_transport_daily_metrics": "load_date",
    }
    for table, col in date_map.items():
        if table in ds and ds[table]:
            set_main_dttm(token, args.superset_url, ds[table], col)
    print()

    # ── Step 3: Create charts ────────────────────────────────────────────────
    print("--- Step 3: Create Charts ---")

    # ═══ HR & LABOR DASHBOARD ════════════════════════════════════════════════
    hr_charts = []

    hr_sections = [
        {
            "key": "mart_department_labor",
            "name": "Attendance Rate by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("attendance_rate_pct", "AVG")],
                "groupby": ["department"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_department_labor",
            "name": "Hours Utilization by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("hours_utilization_pct", "AVG")],
                "groupby": ["department"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_department_labor",
            "name": "Cost Variance by Location",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("cost_variance", "SUM")],
                "groupby": ["location_name"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_labor_efficiency",
            "name": "Shift Completion Rate",
            "viz": "big_number_total",
            "params": {
                "metric": make_metric("completion_rate_pct", "AVG", label="Avg Completion %"),
                "subheader": "Shift Completion Rate",
            },
        },
        {
            "key": "mart_labor_efficiency",
            "name": "No-Show Rate by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("no_show_rate_pct", "AVG")],
                "groupby": ["department"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_employee_productivity",
            "name": "Revenue per Hour by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("revenue_per_hour", "AVG")],
                "groupby": ["department"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_employee_cost",
            "name": "Labor Cost % of Revenue",
            "viz": "line",
            "params": {
                "metrics": [make_metric("labor_pct_of_revenue", "AVG")],
                "groupby": ["location_name"],
                "row_limit": 365,
            },
        },
        {
            "key": "mart_loyalty_cohort",
            "name": "Member Count by Tier",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("member_id", "COUNT_DISTINCT")],
                "groupby": ["loyalty_tier"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_loyalty_cohort",
            "name": "Avg Spend by Tier",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("tier_avg_spend", "AVG")],
                "groupby": ["loyalty_tier"],
                "row_limit": 10,
            },
        },
    ]
    for sec in hr_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                hr_charts.append(c)

    # ═══ WORKFORCE / ATTENDANCE / LABOR COST DASHBOARD (data-lab#28) ════════
    # Re-uses existing HR/Labor datasets (mart_department_labor, mart_labor_efficiency,
    # mart_employee_productivity, mart_employee_cost) plus the two new marts.
    wf_charts = []
    wf_sections = [
        {
            "key": "mart_department_labor",
            "name": "Scheduled vs Actual Hours by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("scheduled_hours", "SUM"), make_metric("actual_hours", "SUM")],
                "groupby": ["department"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_labor_cost_by_department",
            "name": "Actual Labor Cost by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("actual_cost", "SUM")],
                "groupby": ["department"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_labor_cost_by_department",
            "name": "Labor Cost % of Revenue",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("labor_cost_pct_of_revenue", "AVG")],
                "groupby": ["department"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_attendance_compliance",
            "name": "No-Show Rate by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("no_show_rate_pct", "AVG")],
                "groupby": ["department"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_attendance_compliance",
            "name": "Late-Arrival Rate by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("late_arrival_rate_pct", "AVG")],
                "groupby": ["department"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_attendance_compliance",
            "name": "Break Compliance Rate",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("break_compliance_rate_pct", "AVG")],
                "groupby": ["department"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_attendance_compliance",
            "name": "Overtime Rate by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("overtime_rate_pct", "AVG")],
                "groupby": ["department"],
                "row_limit": 20,
            },
        },
    ]
    for sec in wf_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                wf_charts.append(c)

    # ═══ SHRINK & LOSS DASHBOARD ═════════════════════════════════════════════
    shrink_charts = []
    shrink_sections = [
        {
            "key": "mart_shrink_analysis",
            "name": "Shrink Value Lost by Type",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("total_value_lost", "SUM")],
                "groupby": ["shrinkage_type"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_shrink_analysis",
            "name": "Shrink Value Lost by Location",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_value_lost", "SUM")],
                "groupby": ["location_name"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_shrink_analysis",
            "name": "Daily Shrink Trend",
            "viz": "echarts_timeseries_line",
            "params": {
                "metrics": [make_metric("total_value_lost", "SUM")],
                "groupby": ["shrinkage_type"],
                "granularity_sqla": "event_date",
                "time_grain_sqla": "P1D",
                "row_limit": 10000,
            },
        },
        {
            "key": "mart_department_shrinkage",
            "name": "Shrink % of Revenue by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("shrink_pct_of_revenue", "AVG")],
                "groupby": ["department_name"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_department_shrinkage",
            "name": "Value Lost by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_value_lost", "SUM")],
                "groupby": ["department_name"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_promotion_effectiveness",
            "name": "Promotion Revenue Impact",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_revenue", "SUM")],
                "groupby": ["ad_name"],
                "row_limit": 20,
            },
        },
    ]
    for sec in shrink_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                shrink_charts.append(c)

    # ═══ SUPPLY CHAIN DASHBOARD (data-lab#29) ══════════════════════════════
    # Register the new #29 marts as datasets and build charts for the dedicated
    # store_supply_chain dashboard (replacing the legacy supply-chain slug).
    for t in ("mart_supply_chain_kpis", "mart_order_cycle_time", "mart_order_fulfillment_funnel"):
        if t not in ds:
            ds_id = register_dataset(token, args.superset_url, grocery_db_id, t)
            if ds_id:
                ds[t] = ds_id
    sc_date_map = {
        "mart_supply_chain_kpis": "report_date",
        "mart_order_cycle_time": "order_date",
        "mart_order_fulfillment_funnel": "order_date",
    }
    for t, col in sc_date_map.items():
        if t in ds and ds[t]:
            set_main_dttm(token, args.superset_url, ds[t], col)

    supply_charts = []
    supply_sections = [
        {
            "key": "mart_supply_chain_summary",
            "name": "Order-to-Delivery Days (All Stages)",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("days_order_to_delivery", "AVG")],
                "groupby": ["pipeline_stage"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_supply_chain_summary",
            "name": "Pipeline Stage Distribution (Supply Chain)",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("order_id", "COUNT_DISTINCT")],
                "groupby": ["pipeline_stage"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_supply_chain_summary",
            "name": "Days to Approve by Status",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("hours_to_approve", "AVG")],
                "groupby": ["order_status"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_supply_chain_summary",
            "name": "Hours in Transit by Route",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("hours_in_transit", "AVG")],
                "groupby": ["load_status"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_supply_chain_kpis",
            "name": "Fill Rate %",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("fill_rate_pct", "AVG")],
                "groupby": ["warehouse_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_supply_chain_kpis",
            "name": "On-Time Delivery Rate %",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("on_time_rate_pct", "AVG")],
                "groupby": ["warehouse_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_supply_chain_kpis",
            "name": "Short Rate %",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("short_rate_pct", "AVG")],
                "groupby": ["warehouse_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_order_cycle_time",
            "name": "Avg Order→Fulfillment Hours",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("order_to_fulfill_hours", "AVG")],
                "groupby": ["warehouse_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_order_cycle_time",
            "name": "Avg Order→Delivery Hours",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_order_to_delivery_hours", "AVG")],
                "groupby": ["store_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_fulfillment_pick_accuracy",
            "name": "Pick Accuracy (Fill Rate %)",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_fill_rate_pct", "AVG")],
                "groupby": ["warehouse_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_order_fulfillment_funnel",
            "name": "SLA Breaches (Late Deliveries)",
            "viz": "big_number_total",
            "params": {
                "metric": make_metric("days_late", "AVG", label="Avg Days Late (SLA Breach)"),
                "subheader": "Avg Days Late on Breached Orders",
            },
        },
    ]
    for sec in supply_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                supply_charts.append(c)

    # ═══ TRANSPORT & FLEET DASHBOARD (data-lab#30) ══════════════════════════
    transport_charts = []
    transport_sections = [
        {
            "key": "mart_route_efficiency",
            "name": "On-Time Rate % by Route",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("on_time_rate_pct", "AVG")],
                "groupby": ["store_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_route_efficiency",
            "name": "Estimated Route Labor Cost",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("estimated_route_cost_labor", "SUM")],
                "groupby": ["store_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_route_efficiency",
            "name": "Avg Transit Hours by Route",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("avg_transit_hours", "AVG")],
                "groupby": ["store_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_fleet_cost",
            "name": "Estimated Labor Cost by Truck/Driver",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("estimated_labor_cost", "SUM")],
                "groupby": ["driver_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_fleet_utilization",
            "name": "Fleet On-Time Rate %",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("on_time_rate_pct", "AVG")],
                "groupby": ["utilization_status"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_fleet_utilization",
            "name": "Total Loads by Truck",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_loads", "SUM")],
                "groupby": ["license_plate"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_transport_daily_metrics",
            "name": "Daily Load Volume",
            "viz": "echarts_timeseries_line",
            "params": {
                "metrics": [make_metric("total_loads", "SUM")],
                "groupby": ["warehouse_name"],
                "granularity_sqla": "load_date",
                "time_grain_sqla": "P1D",
                "row_limit": 10000,
            },
        },
        {
            "key": "mart_transport_daily_metrics",
            "name": "Daily Completion Rate %",
            "viz": "echarts_timeseries_line",
            "params": {
                "metrics": [make_metric("completion_rate_pct", "AVG")],
                "groupby": ["warehouse_name"],
                "granularity_sqla": "load_date",
                "time_grain_sqla": "P1D",
                "row_limit": 10000,
            },
        },
    ]
    for sec in transport_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                transport_charts.append(c)

    print()

    # ═══ LOYALTY & RETENTION DASHBOARD (data-lab#31) ═════════════════════
    loyalty_charts = []
    loyalty_sections = [
        {
            "key": "mart_loyalty_rfm",
            "name": "RFM Segment Distribution",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("member_id", "COUNT_DISTINCT")],
                "groupby": ["rfm_segment"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_loyalty_rfm",
            "name": "Avg Monetary Value by Segment",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("monetary", "AVG")],
                "groupby": ["rfm_segment"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_loyalty_rfm",
            "name": "Avg Frequency by Segment",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("frequency", "AVG")],
                "groupby": ["rfm_segment"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_loyalty_engagement",
            "name": "Active Rate %",
            "viz": "big_number_total",
            "params": {
                "metric": make_metric("active_rate_pct", "AVG", label="Active Rate %"),
                "subheader": "Share of members active in last 90 days",
            },
        },
        {
            "key": "mart_loyalty_engagement",
            "name": "Redemption Rate %",
            "viz": "big_number_total",
            "params": {
                "metric": make_metric("redemption_rate_pct", "AVG", label="Redemption Rate %"),
                "subheader": "Points redeemed / points earned",
            },
        },
        {
            "key": "mart_loyalty_engagement",
            "name": "Points Liability (Outstanding)",
            "viz": "big_number_total",
            "params": {
                "metric": make_metric("points_liability", "SUM", label="Outstanding Points Liability"),
                "subheader": "Sum of active member point balances",
            },
        },
        {
            "key": "mart_loyalty_engagement",
            "name": "Tier Migration (Upgrades)",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("tier_upgrades", "SUM")],
                "groupby": ["current_tier"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_loyalty_cohort",
            "name": "Members by Tier",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("member_count", "SUM")],
                "groupby": ["loyalty_tier"],
                "row_limit": 10,
            },
        },
    ]
    for sec in loyalty_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                loyalty_charts.append(c)

    print()

    # ═══ POS, SALES & PROMOTIONS DASHBOARD (data-lab#26) ════════════════════
    pos_charts = []
    pos_sections = [
        {
            "key": "mart_daily_revenue",
            "name": "Daily Revenue Trend",
            "viz": "echarts_timeseries_line",
            "params": {
                "metrics": [make_metric("total_revenue", "SUM")],
                "groupby": ["location_name"],
                "granularity_sqla": "transaction_date",
                "time_grain_sqla": "P1D",
                "row_limit": 10000,
            },
        },
        {
            "key": "mart_category_sales_performance",
            "name": "Category Revenue",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("category_revenue", "SUM")],
                "groupby": ["category"],
                "row_limit": 50,
            },
        },
        {
            "key": "mart_category_sales_performance",
            "name": "Avg Basket Size by Category",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("avg_basket_size", "AVG")],
                "groupby": ["category"],
                "row_limit": 50,
            },
        },
        {
            "key": "mart_category_sales_performance",
            "name": "Units Sold by Category",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("category_units", "SUM")],
                "groupby": ["category"],
                "row_limit": 50,
            },
        },
        {
            "key": "mart_promotion_effectiveness",
            "name": "Promo Lift vs Baseline %",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("promo_lift_vs_baseline_pct", "AVG")],
                "groupby": ["ad_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_promotion_redemption",
            "name": "Coupon Redemption Rate",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("redemption_rate_pct", "AVG")],
                "groupby": ["promotion_name"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_promotion_redemption",
            "name": "Coupon vs Combo Redemptions",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("coupon_txn_count", "SUM")],
                "groupby": ["promotion_type"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_product_price_elasticity",
            "name": "Price Elasticity Explorer",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("price_elasticity", "AVG")],
                "groupby": ["product_id"],
                "row_limit": 50,
            },
        },
    ]
    for sec in pos_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                pos_charts.append(c)

    # ═══ INVENTORY, STOCK & SHRINKAGE DASHBOARD (data-lab#27) ═══════════════
    inv_charts = []
    inv_sections = [
        {
            "key": "mart_inventory_valuation",
            "name": "On-Hand Value by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("value_on_hand_cost", "SUM")],
                "groupby": ["department_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_inventory_valuation",
            "name": "On-Hand Value by Location",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("value_on_hand_cost", "SUM")],
                "groupby": ["location_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_shrinkage_analysis",
            "name": "Shrink Value by Cause",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("total_value_lost", "SUM")],
                "groupby": ["shrinkage_type"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_shrinkage_analysis",
            "name": "Shrink Value by Department",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("total_value_lost", "SUM")],
                "groupby": ["department_name"],
                "row_limit": 20,
            },
        },
        {
            "key": "mart_inventory_turnover",
            "name": "Stock Aging Distribution",
            "viz": "pie",
            "params": {
                "metrics": [make_metric("product_id", "COUNT_DISTINCT")],
                "groupby": ["stock_aging_category"],
                "row_limit": 10,
            },
        },
        {
            "key": "mart_inventory_turnover",
            "name": "Estimated Annual Turnover",
            "viz": "bar",
            "params": {
                "metrics": [make_metric("estimated_annual_turnover", "AVG")],
                "groupby": ["department_name"],
                "row_limit": 20,
            },
        },
    ]
    for sec in inv_sections:
        ds_id = ds.get(sec["key"])
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"], sec["params"])
            if c:
                inv_charts.append(c)

    # ═══ EXECUTIVE MASTER DASHBOARD (data-lab#32) ══════════════════════════
    # Top-line KPIs across all six domains on one screen. Reads the same marts
    # the per-domain dashboards use (registry contract in reporting/registry.yml).
    exec_tables = [
        "mart_daily_revenue",
        "mart_inventory_valuation",
        "mart_shrinkage_analysis",
        "mart_labor_cost_by_department",
        "mart_supply_chain_kpis",
        "mart_route_efficiency",
        "mart_loyalty_engagement",
    ]
    exec_dttm = {
        "mart_daily_revenue": "transaction_date",
        "mart_inventory_valuation": "location_id",
        "mart_shrinkage_analysis": "event_date",
        "mart_labor_cost_by_department": "report_date",
        "mart_supply_chain_kpis": "report_date",
        "mart_route_efficiency": "warehouse_name",
        "mart_loyalty_engagement": None,
    }
    for t in exec_tables:
        if t not in ds:
            ds_id = register_dataset(token, args.superset_url, grocery_db_id, t)
            if ds_id:
                ds[t] = ds_id
    for t, col in exec_dttm.items():
        if t in ds and ds[t] and col:
            set_main_dttm(token, args.superset_url, ds[t], col)

    exec_charts = []
    exec_kpis = [
        ("Exec: Total Revenue", "mart_daily_revenue", "total_revenue", "SUM", "$,.0f", "All-time POS revenue"),
        ("Exec: Inventory On-Hand Value", "mart_inventory_valuation", "value_on_hand_cost", "SUM", "$,.0f", "Cost value of stock on hand"),
        ("Exec: Shrink Value Lost", "mart_shrinkage_analysis", "total_value_lost", "SUM", "$,.0f", "Total shrinkage $ lost"),
        ("Exec: Labor Cost", "mart_labor_cost_by_department", "actual_cost", "SUM", "$,.0f", "Actual labor cost (dept/day)"),
        ("Exec: Supply Chain Fill Rate", "mart_supply_chain_kpis", "fill_rate_pct", "AVG", ".1f", "Order fill rate %"),
        ("Exec: Transport On-Time %", "mart_route_efficiency", "on_time_rate_pct", "AVG", ".1f", "Load on-time delivery %"),
        ("Exec: Loyalty Active Rate", "mart_loyalty_engagement", "active_rate_pct", "AVG", ".1f", "Members active in 90d"),
        ("Exec: Points Liability", "mart_loyalty_engagement", "points_liability", "AVG", ",.0f", "Outstanding points obligation"),
    ]
    for name, tbl, col, agg, fmt, sub in exec_kpis:
        ds_id = ds.get(tbl)
        if ds_id:
            c = create_chart(token, args.superset_url, ds_id, name, "big_number_total", {
                "metric": make_metric(col, agg),
                "subheader": sub,
                "y_axis_format": fmt,
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
            })
            if c:
                exec_charts.append(c)

    print()

    # ── Step 4: Create dashboards ────────────────────────────────────────────
    print("--- Step 4: Create Dashboards ---")

    if hr_charts or wf_charts:
        # hr_charts = existing HR/Labor charts; wf_charts = new workforce/compliance charts
        create_dashboard(token, args.superset_url, hr_charts + wf_charts, "Store — Workforce, Attendance & Labor Cost", "store_hr_labor")
    if shrink_charts:
        create_dashboard(token, args.superset_url, shrink_charts, "Shrink & Promotions", "shrink-promotions")
    if supply_charts:
        create_dashboard(token, args.superset_url, supply_charts, "Store — Supply Chain & Fulfillment", "store_supply_chain")
    if pos_charts:
        create_dashboard(token, args.superset_url, pos_charts, "Store — Sales, Promotions & Pricing", "store_pos_promotions")
    if inv_charts:
        create_dashboard(token, args.superset_url, inv_charts, "Store — Inventory, Stock & Shrinkage", "store_inventory_shrinkage")
    if transport_charts:
        create_dashboard(token, args.superset_url, transport_charts, "Store — Transport & Fleet", "store_transport_fleet")
    if loyalty_charts:
        create_dashboard(token, args.superset_url, loyalty_charts, "Store — Loyalty & Retention", "store_loyalty_crm")
    if exec_charts:
        create_dashboard(token, args.superset_url, exec_charts, "Store — Executive Performance Overview", "store_performance_exec")

    print("\n=== Done ===")
    print(f"\nSummary:")
    print(f"  HR & Labor:        {len(hr_charts)} charts")
    print(f"  Shrink & Promo:    {len(shrink_charts)} charts")
    print(f"  Supply Chain:      {len(supply_charts)} charts")
    print(f"  POS & Promotions:  {len(pos_charts)} charts")
    print(f"  Inventory & Shrink: {len(inv_charts)} charts")
    print(f"  Transport & Fleet:  {len(transport_charts)} charts")
    print(f"  Loyalty & Retention: {len(loyalty_charts)} charts")
    print(f"  Executive Overview:  {len(exec_charts)} charts")


if __name__ == "__main__":
    main()
