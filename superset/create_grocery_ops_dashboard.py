#!/usr/bin/env python3
"""
Create Grocery Operations dashboard in Superset via REST API.

Creates datasets for 4 new mart tables, builds charts, assembles a dashboard.

Usage:
  python3 create_grocery_ops_dashboard.py [--superset-url URL] [--username USER] [--password PASS]
"""

import json
import sys
import os
import uuid
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("Installing requests...")
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

# ── Config ──────────────────────────────────────────────────────────────────
SUPERSET_URL = "http://192.168.1.4:8088"
USERNAME = "admin"
PASSWORD = "admin"

GROCERY_DB_ID = 3

# Known dataset IDs (set after first run)
DS_IDS = {
    "mart_inventory_turnover": 15,
    "mart_hourly_sales_pattern": 16,
    "mart_delivery_performance": 17,
    "mart_store_weekly_summary": 18,
}

# ── Auth ─────────────────────────────────────────────────────────────────────
def get_token(url, username, password):
    resp = requests.post(urljoin(url, "/api/v1/security/login"),
        json={"username": username, "password": password, "provider": "db"}, timeout=10)
    resp.raise_for_status()
    return resp.json()["access_token"]

def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ── Set main_dttm_col on datasets ────────────────────────────────────────────
def set_main_dttm(token, ds_id, col):
    """Set the main datetime column on a dataset via PUT."""
    resp = requests.put(urljoin(SUPERSET_URL, f"/api/v1/dataset/{ds_id}"),
        headers=headers(token),
        json={"main_dttm_col": col}, timeout=10)
    if resp.status_code == 200:
        print(f"  ✓ Set main_dttm_col='{col}' on dataset {ds_id}")
    else:
        print(f"  ✗ Failed: {resp.status_code} {resp.text[:200]}")

# ── Chart definitions ──────────────────────────────────────────────────────
def build_chart_payload(ds_id, slice_name, viz_type, params_extra):
    """Build a chart payload dict."""
    base_params = {
        "datasource": f"{ds_id}__table",
        "viz_type": viz_type,
        "time_range": "No filter",
        "datasource_type": "table",
        "adhoc_filters": [],
    }
    base_params.update(params_extra)
    return {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(base_params),
        "dashboards": [],
    }

def make_metric(col, agg="SUM", label=None):
    """Helper to create a metric dict."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col, "type": ""},
        "aggregate": agg,
        "label": label or f"{agg}({col})",
    }

def make_filter(col, op, val, clause="WHERE"):
    """Helper to create an adhoc filter."""
    return {
        "expressionType": "SIMPLE",
        "subject": col,
        "operator": op,
        "comparator": val,
        "clause": clause,
    }

CHARTS = [
    # ── Row 1: Inventory Health KPIs ──
    {
        "ds_key": "mart_inventory_turnover",
        "slice_name": "Stock Aging Breakdown",
        "viz_type": "pie",
        "params": {
            "metrics": [make_metric("quantity_on_hand", "SUM")],
            "groupby": ["stock_aging_category"],
            "show_labels": True,
            "number_format": "SMART_NUMBER",
            "label_type": "key_value_percent",
            "show_legend": True,
            "row_limit": 10,
            "outerRadius": 70,
            "innerRadius": 30,
        },
    },
    {
        "ds_key": "mart_inventory_turnover",
        "slice_name": "Stock Alerts",
        "viz_type": "big_number_total",
        "params": {
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "quantity_on_hand", "type": ""},
                "aggregate": "COUNT",
                "label": "COUNT(*)",
            },
            "adhoc_filters": [
                make_filter("stock_aging_category", "IN", ["REORDER_NEEDED", "OUT_OF_STOCK"]),
            ],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "subheader": "Items Below Reorder or Out of Stock",
            "y_axis_format": ",.0f",
        },
    },
    {
        "ds_key": "mart_inventory_turnover",
        "slice_name": "Avg Days of Supply by Category",
        "viz_type": "dist_bar",
        "params": {
            "metrics": [make_metric("days_of_supply", "AVG")],
            "groupby": ["stock_aging_category"],
            "adhoc_filters": [make_filter("days_of_supply", "IS NOT NULL", None)],
            "row_limit": 10,
            "order_bars": True,
            "y_axis_format": ",.1f",
            "show_legend": False,
            "color_scheme": "supersetColors",
        },
    },
    # ── Row 2: Hourly Patterns ──
    {
        "ds_key": "mart_hourly_sales_pattern",
        "slice_name": "Peak Hours Heatmap",
        "viz_type": "heatmap",
        "params": {
            "all_columns_x": "hour_of_day",
            "all_columns_y": "day_name",
            "metric": make_metric("total_revenue", "SUM"),
            "linear_color_scheme": "blue_white_yellow",
            "normalize_across": "heatmap",
            "y_axis_format": "$,.0f",
            "show_values": True,
        },
    },
    {
        "ds_key": "mart_hourly_sales_pattern",
        "slice_name": "Weekend Revenue by Hour",
        "viz_type": "dist_bar",
        "params": {
            "metrics": [make_metric("total_revenue", "SUM")],
            "groupby": ["hour_of_day"],
            "columns": ["location_name"],
            "adhoc_filters": [make_filter("day_name", "IN", ["Saturday", "Sunday"])],
            "row_limit": 24,
            "order_bars": False,
            "bar_stacked": True,
            "y_axis_format": "$,.0f",
            "show_legend": True,
            "color_scheme": "supersetColors",
        },
    },
    # ── Row 3: Delivery SLA ──
    {
        "ds_key": "mart_delivery_performance",
        "slice_name": "Delivery SLA Trend",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "granularity_sqla": "report_week_start",
            "time_grain_sqla": "P1W",
            "metrics": [
                make_metric("on_time_delivery_pct", "AVG"),
                make_metric("total_orders", "SUM"),
            ],
            "groupby": ["warehouse_name"],
            "seriesType": "line",
            "showLegend": True,
            "y_axis_format": ",.1f",
            "x_axis_time_format": "smart_date",
            "zoomable": True,
        },
    },
    # ── Row 4: Store Weekly KPIs ──
    {
        "ds_key": "mart_store_weekly_summary",
        "slice_name": "Weekly Revenue by Store",
        "viz_type": "dist_bar",
        "params": {
            "granularity_sqla": "week_start",
            "metrics": [make_metric("total_revenue", "SUM")],
            "groupby": ["location_name"],
            "row_limit": 50,
            "order_bars": True,
            "y_axis_format": "$,.0f",
            "show_legend": True,
            "color_scheme": "supersetColors",
        },
    },
    {
        "ds_key": "mart_store_weekly_summary",
        "slice_name": "Shrink % of Revenue Trend",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "granularity_sqla": "week_start",
            "time_grain_sqla": "P1W",
            "metrics": [make_metric("shrink_pct_of_revenue", "AVG")],
            "groupby": ["location_name"],
            "seriesType": "line",
            "showLegend": True,
            "y_axis_format": ",.1f",
            "x_axis_time_format": "smart_date",
            "zoomable": True,
        },
    },
    {
        "ds_key": "mart_store_weekly_summary",
        "slice_name": "Out of Stock Count Trend",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "granularity_sqla": "week_start",
            "time_grain_sqla": "P1W",
            "metrics": [make_metric("out_of_stock_count", "SUM")],
            "groupby": ["location_name"],
            "seriesType": "line",
            "showLegend": True,
            "y_axis_format": ",.0f",
            "x_axis_time_format": "smart_date",
            "zoomable": True,
        },
    },
    {
        "ds_key": "mart_store_weekly_summary",
        "slice_name": "Labor Cost % of Revenue",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "granularity_sqla": "week_start",
            "time_grain_sqla": "P1W",
            "metrics": [make_metric("labor_cost_pct_of_revenue", "AVG")],
            "groupby": ["location_name"],
            "seriesType": "line",
            "showLegend": True,
            "y_axis_format": ",.2f",
            "x_axis_time_format": "smart_date",
            "zoomable": True,
        },
    },
]

def create_chart(token, chart_def):
    ds_id = DS_IDS.get(chart_def["ds_key"])
    if not ds_id:
        print(f"  ✗ No dataset ID for '{chart_def['ds_key']}'")
        return None

    payload = build_chart_payload(
        ds_id,
        chart_def["slice_name"],
        chart_def["viz_type"],
        chart_def["params"],
    )

    resp = requests.post(urljoin(SUPERSET_URL, "/api/v1/chart/"),
        headers=headers(token), json=payload, timeout=30)

    if resp.status_code == 201:
        result = resp.json()
        cid = result["id"]
        cname = result.get("result", {}).get("slice_name", chart_def["slice_name"])
        print(f"  ✓ Chart '{cname}' (ID={cid})")
        return {"id": cid, "slice_name": cname}
    else:
        print(f"  ✗ Failed '{chart_def['slice_name']}': {resp.status_code} {resp.text[:300]}")
        return None

def create_dashboard(token, chart_ids):
    """Create dashboard with charts arranged in rows of 3."""
    import random, string
    
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

        for j, (cid, cname) in enumerate(row_charts):
            ckey = f"CHART-{cid}"
            position[ckey] = {
                "type": "CHART", "id": ckey, "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": cid, "width": 4, "height": 60, "sliceName": cname},
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
        "dashboard_title": "Grocery Operations",
        "slug": "grocery-operations",
        "published": True,
        "position_json": json.dumps(position),
        "json_metadata": json.dumps({
            "chart_configuration": {},
            "global_chart_configuration": {
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": [c[0] for c in chart_ids],
            },
            "refresh_frequency": 0,
            "color_scheme": "",
            "label_colors": {},
            "cross_filters_enabled": True,
        }),
    }

    resp = requests.post(urljoin(SUPERSET_URL, "/api/v1/dashboard/"),
        headers=headers(token), json=payload, timeout=30)

    if resp.status_code == 201:
        result = resp.json()
        print(f"  ✓ Dashboard 'Grocery Operations' (ID={result['id']})")
        return result
    else:
        # Dashboard may already exist — try fetching it
        if resp.status_code == 422:
            list_r = requests.get(urljoin(SUPERSET_URL, "/api/v1/dashboard/"),
                headers=headers(token),
                params={"q": '(page:0,page_size:50,filters:!((col:slug,opr:eq,value:grocery-operations)))'},
                timeout=10)
            if list_r.status_code == 200:
                for d in list_r.json().get("result", []):
                    if d.get("slug") == "grocery-operations":
                        print(f"  ℹ Dashboard already exists (ID={d['id']}) — updating with PUT")
                        resp2 = requests.put(urljoin(SUPERSET_URL, f"/api/v1/dashboard/{d['id']}"),
                            headers=headers(token), json=payload, timeout=30)
                        if resp2.status_code == 200:
                            print(f"  ✓ Dashboard updated (ID={d['id']})")
                            return resp2.json()
                        else:
                            print(f"  ✗ Update failed: {resp2.status_code} {resp2.text[:300]}")
                            return None
        print(f"  ✗ Dashboard create/update failed: {resp.status_code} {resp.text[:400]}")
        return None


def main():
    print("=" * 60)
    print("Grocery Operations Dashboard Creator")
    print("=" * 60)
    
    token = get_token(SUPERSET_URL, USERNAME, PASSWORD)
    print("✓ Authenticated\n")

    # Step 1: Set main_dttm_col on datasets that have date columns
    print("Step 1: Configure dataset date columns...")
    set_main_dttm(token, 17, "report_week_start")   # mart_delivery_performance
    set_main_dttm(token, 18, "week_start")           # mart_store_weekly_summary
    # mart_inventory_turnover has last_updated but it's not really a time-dim column
    # mart_hourly_sales_pattern has no date column
    print()

    # Step 2: Create charts
    print("Step 2: Creating charts...")
    chart_results = []
    for ch in CHARTS:
        result = create_chart(token, ch)
        if result:
            chart_results.append((result["id"], result["slice_name"]))
    print(f"  Charts created: {len(chart_results)}/{len(CHARTS)}\n")

    # Step 3: Create/update dashboard
    print("Step 3: Creating dashboard...")
    dash = create_dashboard(token, chart_results)
    if dash:
        dash_id = dash.get("id") or (dash.get("result", {}) or {}).get("id", "?")
        print(f"\n✅ Dashboard ready!")
        print(f"   URL: {SUPERSET_URL}/superset/dashboard/{dash_id}/")
    else:
        print("❌ Dashboard creation failed. Charts are still available individually.")
        for cid, cname in chart_results:
            print(f"   Chart: {SUPERSET_URL}/explore/?datasource_type=table&datasource_id={cid}")

    print("\nDone.")


if __name__ == "__main__":
    main()
