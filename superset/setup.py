#!/usr/bin/env python3
"""
Superset setup script — creates database connections, mart datasets, charts,
and dashboards for Grocery via the REST API.

Idempotent: checks for existing objects before creating.
Run this after the superset service is healthy.
"""

import json
import os
import sys
import time

import requests

BASE = "http://superset:8088"
USERNAME = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

GROCERY_URI = (
    f"postgresql+psycopg2://"
    f"{os.environ.get('POSTGRES_USER', 'postgres')}:"
    f"{os.environ.get('POSTGRES_PASSWORD', 'postgres')}"
    f"@postgres:5432/grocery"
)

# ── Auth ──────────────────────────────────────────────────────────────────────

def login():
    r = requests.post(f"{BASE}/api/v1/security/login", json={
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True,
    })
    r.raise_for_status()
    return r.json()["access_token"]


def h(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def wait_for_superset(retries=30, delay=5):
    print(f"Waiting for Superset at {BASE} ...")
    for i in range(retries):
        try:
            r = requests.get(f"{BASE}/health", timeout=5)
            if r.status_code == 200:
                print("Superset is up.")
                return
        except Exception:
            pass
        print(f"  Not ready yet ({i+1}/{retries}), retrying in {delay}s...")
        time.sleep(delay)
    print("ERROR: Superset did not become ready in time.", file=sys.stderr)
    sys.exit(1)


# ── Charts ────────────────────────────────────────────────────────────────────

def params_big_number(ds_id, column, aggregate, fmt, subheader="", force_timestamp_formatting=False):
    return json.dumps({
        "viz_type": "big_number_total",
        "datasource": f"{ds_id}__table",
        "metric": {
            "expressionType": "SIMPLE",
            "column": {"column_name": column, "type": "NUMERIC"},
            "aggregate": aggregate,
            "label": f"{aggregate}({column})",
        },
        "adhoc_filters": [],
        "time_range": "No filter",
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "subheader": subheader,
        "y_axis_format": fmt,
        "force_timestamp_formatting": force_timestamp_formatting,
    })


def params_line(ds_id, time_col, metrics, groupby, fmt="$,.0f"):
    return json.dumps({
        "viz_type": "echarts_timeseries_line",
        "datasource": f"{ds_id}__table",
        "granularity_sqla": time_col,
        "time_grain_sqla": "P1D",
        "time_range": "No filter",
        "metrics": metrics,
        "groupby": groupby,
        "adhoc_filters": [],
        "seriesType": "line",
        "showLegend": True,
        "smooth": False,
        "stack": None,
        "area": False,
        "markerEnabled": False,
        "y_axis_format": fmt,
        "x_axis_time_format": "smart_date",
        "rowLimit": 10000,
        "zoomable": True,
        "truncate_metric": True,
        "show_empty_columns": True,
    })


def params_timeseries_bar(ds_id, time_col, metrics, groupby, fmt="$,.0f"):
    return json.dumps({
        "viz_type": "echarts_timeseries_bar",
        "datasource": f"{ds_id}__table",
        "granularity_sqla": time_col,
        "time_grain_sqla": "P1D",
        "time_range": "No filter",
        "metrics": metrics,
        "groupby": groupby,
        "adhoc_filters": [],
        "stack": True,
        "showLegend": True,
        "y_axis_format": fmt,
        "x_axis_time_format": "smart_date",
        "rowLimit": 10000,
        "zoomable": True,
    })


def params_dist_bar(ds_id, metrics, groupby, fmt="$,.0f"):
    return json.dumps({
        "viz_type": "dist_bar",
        "datasource": f"{ds_id}__table",
        "metrics": metrics,
        "adhoc_filters": [],
        "groupby": groupby,
        "columns": [],
        "row_limit": 50,
        "order_bars": True,
        "y_axis_format": fmt,
        "bottom_margin": "auto",
        "x_ticks_layout": "auto",
        "show_legend": True,
        "bar_stacked": False,
        "color_scheme": "supersetColors",
    })


def params_pie(ds_id, groupby, metric, fmt=".1f"):
    return json.dumps({
        "viz_type": "pie",
        "datasource": f"{ds_id}__table",
        "groupby": groupby,
        "metric": metric,
        "adhoc_filters": [],
        "row_limit": 10,
        "sort_by_metric": True,
        "donut": True,
        "show_legend": True,
        "show_labels": True,
        "labels_outside": True,
        "label_type": "key_percent",
        "number_format": fmt,
        "color_scheme": "supersetColors",
    })


def simple_metric(column, aggregate="SUM"):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column, "type": "NUMERIC"},
        "aggregate": aggregate,
        "label": f"{aggregate}({column})",
    }


# ── Dashboard ─────────────────────────────────────────────────────────────────

def build_position(layout):
    """
    layout: list of rows, each row is list of (chart_id, name, width, height)
    Returns position_json dict.
    """
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
    }
    for row_idx, row_charts in enumerate(layout):
        row_id = f"ROW-{row_idx}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart_id, name, width, height in row_charts:
            key = f"CHART-{chart_id}"
            pos[row_id]["children"].append(key)
            pos[key] = {
                "type": "CHART",
                "id": key,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": chart_id,
                    "width": width,
                    "height": height,
                    "sliceName": name,
                },
            }
    return pos


def link_charts_to_dashboard(dash_id, chart_ids):
    """
    The REST API does not expose the dashboard→slices M2M relationship.
    Use the Superset Python models directly to populate dashboard_slices.
    """
    try:
        import os
        os.environ.setdefault("SUPERSET_CONFIG_PATH", "/app/superset_home/superset_config.py")
        from superset import create_app as _create_app
        _app = _create_app()
        with _app.app_context():
            from superset.models.dashboard import Dashboard
            from superset.models.slice import Slice
            from superset.extensions import db
            dashboard = db.session.get(Dashboard, dash_id)
            slices = db.session.query(Slice).filter(Slice.id.in_(chart_ids)).all()
            dashboard.slices = slices
            db.session.commit()
            print(f"  Linked {len(slices)} charts to dashboard.")
    except Exception as e:
        print(f"  WARNING: could not link charts via Python API: {e}")


# ── Grocery database ──────────────────────────────────────────────────────────

def get_or_create_grocery_database(token):
    r = requests.get(f"{BASE}/api/v1/database/", headers=h(token))
    r.raise_for_status()
    for db in r.json().get("result", []):
        if db["database_name"] == "Grocery":
            print(f"  Database 'Grocery' already exists (id={db['id']})")
            return db["id"]

    r = requests.post(f"{BASE}/api/v1/database/", headers=h(token), json={
        "database_name": "Grocery",
        "sqlalchemy_uri": GROCERY_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": True,
        "allow_cvas": True,
        "allow_dml": True,
    })
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"  Created database 'Grocery' (id={db_id})")
    return db_id


# ── Grocery datasets ──────────────────────────────────────────────────────────

GROCERY_MART_TABLES = [
    ("mart_daily_revenue",         "mart", "transaction_date"),
    ("mart_product_performance",   "mart", "transaction_date"),
    ("mart_department_performance","mart", "transaction_date"),
    ("mart_location_performance",  "mart", None),
    ("mart_inventory_summary",     "mart", None),
    ("mart_supply_chain_summary",  "mart", "order_date"),
]


def get_or_create_grocery_datasets(token, db_id):
    r = requests.get(
        f"{BASE}/api/v1/dataset/",
        headers=h(token),
        params={"q": json.dumps({"page_size": 200})},
    )
    r.raise_for_status()
    existing = {
        (d["schema"], d["table_name"], d["database"]["id"]): d["id"]
        for d in r.json().get("result", [])
    }

    datasets = {}
    for table_name, schema, dttm_col in GROCERY_MART_TABLES:
        key = (schema, table_name, db_id)
        if key in existing:
            print(f"  Dataset grocery/{schema}.{table_name} already exists (id={existing[key]})")
            datasets[table_name] = existing[key]
        else:
            payload = {
                "database": db_id,
                "schema": schema,
                "table_name": table_name,
            }
            r = requests.post(f"{BASE}/api/v1/dataset/", headers=h(token), json=payload)
            if r.status_code in (200, 201):
                ds_id = r.json()["id"]
                if dttm_col:
                    requests.put(
                        f"{BASE}/api/v1/dataset/{ds_id}",
                        headers=h(token),
                        json={"main_dttm_col": dttm_col},
                    )
                datasets[table_name] = ds_id
                print(f"  Created dataset grocery/{schema}.{table_name} (id={ds_id})")
            else:
                print(f"  WARNING: could not create grocery/{schema}.{table_name}: {r.text}")
    return datasets


# ── Grocery charts ────────────────────────────────────────────────────────────

def get_or_create_grocery_charts(token, datasets):
    r = requests.get(
        f"{BASE}/api/v1/chart/",
        headers=h(token),
        params={"q": json.dumps({"page_size": 300})},
    )
    r.raise_for_status()
    existing = {c["slice_name"]: c["id"] for c in r.json().get("result", [])}

    daily_id   = datasets.get("mart_daily_revenue")
    dept_id    = datasets.get("mart_department_performance")
    product_id = datasets.get("mart_product_performance")
    loc_id     = datasets.get("mart_location_performance")

    chart_specs = [
        (
            "Grocery Total Revenue",
            "big_number_total",
            loc_id,
            params_big_number(loc_id, "total_revenue", "SUM", "$,.0f", "All Time"),
        ),
        (
            "Grocery Loyalty Attach Rate",
            "big_number_total",
            loc_id,
            params_big_number(loc_id, "loyalty_attach_rate_pct", "AVG", ",.1f", "% loyalty attach rate"),
        ),
        (
            "Grocery Daily Revenue Trend",
            "echarts_timeseries_line",
            daily_id,
            params_line(
                daily_id,
                "transaction_date",
                [simple_metric("total_revenue")],
                ["location_name"],
                "$,.0f",
            ),
        ),
        (
            "Grocery Revenue by Department",
            "dist_bar",
            dept_id,
            params_dist_bar(
                dept_id,
                [simple_metric("net_revenue")],
                ["department_name"],
                "$,.0f",
            ),
        ),
        (
            "Grocery Revenue by Location",
            "dist_bar",
            loc_id,
            params_dist_bar(
                loc_id,
                [simple_metric("total_revenue")],
                ["location_name"],
                "$,.0f",
            ),
        ),
        (
            "Grocery Top Product Categories",
            "dist_bar",
            product_id,
            params_dist_bar(
                product_id,
                [simple_metric("gross_revenue")],
                ["category"],
                "$,.0f",
            ),
        ),
        (
            "Grocery Avg Daily Revenue by Location",
            "dist_bar",
            loc_id,
            params_dist_bar(
                loc_id,
                [simple_metric("avg_daily_revenue", "AVG")],
                ["location_name"],
                "$,.0f",
            ),
        ),
        (
            "Grocery Coupon vs Deal Savings",
            "echarts_timeseries_bar",
            daily_id,
            params_timeseries_bar(
                daily_id,
                "transaction_date",
                [simple_metric("coupon_savings_total"), simple_metric("deal_savings_total")],
                [],
                "$,.0f",
            ),
        ),
    ]

    charts = {}
    for name, viz_type, ds_id, chart_params in chart_specs:
        if ds_id is None:
            print(f"  Skipping chart '{name}' — dataset not available")
            continue
        if name in existing:
            print(f"  Chart '{name}' already exists (id={existing[name]})")
            charts[name] = existing[name]
        else:
            r = requests.post(f"{BASE}/api/v1/chart/", headers=h(token), json={
                "slice_name": name,
                "viz_type": viz_type,
                "datasource_id": ds_id,
                "datasource_type": "table",
                "params": chart_params,
            })
            if r.status_code in (200, 201):
                cid = r.json()["id"]
                charts[name] = cid
                print(f"  Created chart '{name}' (id={cid})")
            else:
                print(f"  WARNING: could not create chart '{name}': {r.text}")
    return charts


# ── Grocery dashboard ─────────────────────────────────────────────────────────

def get_or_create_grocery_dashboard(token, charts):
    TITLE = "Grocery Overview"

    r = requests.get(
        f"{BASE}/api/v1/dashboard/",
        headers=h(token),
        params={"q": json.dumps({"page_size": 100})},
    )
    r.raise_for_status()
    for d in r.json().get("result", []):
        if d["dashboard_title"] == TITLE:
            print(f"  Dashboard '{TITLE}' already exists (id={d['id']})")
            return d["id"]

    # Row 1: Total Revenue (3) | Loyalty Rate (3) | Revenue by Location (6)
    # Row 2: Daily Revenue Trend (8) | Revenue by Department (4)
    # Row 3: Top Product Categories (6) | Coupon vs Deal Savings (6)
    layout = [
        [
            (charts.get("Grocery Total Revenue", 0),        "Grocery Total Revenue",        3, 60),
            (charts.get("Grocery Loyalty Attach Rate", 0),  "Grocery Loyalty Attach Rate",  3, 60),
            (charts.get("Grocery Revenue by Location", 0),  "Grocery Revenue by Location",  6, 60),
        ],
        [
            (charts.get("Grocery Daily Revenue Trend", 0),  "Grocery Daily Revenue Trend",  8, 80),
            (charts.get("Grocery Revenue by Department", 0),"Grocery Revenue by Department",4, 80),
        ],
        [
            (charts.get("Grocery Top Product Categories", 0), "Grocery Top Product Categories", 6, 80),
            (charts.get("Grocery Coupon vs Deal Savings", 0), "Grocery Coupon vs Deal Savings", 6, 80),
        ],
    ]

    position_json = json.dumps(build_position(layout))

    r = requests.post(f"{BASE}/api/v1/dashboard/", headers=h(token), json={
        "dashboard_title": TITLE,
        "position_json": position_json,
        "published": True,
    })
    if r.status_code not in (200, 201):
        print(f"  WARNING: could not create dashboard: {r.text}")
        return None

    dash_id = r.json()["id"]
    print(f"  Created dashboard '{TITLE}' (id={dash_id})")
    return dash_id


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    wait_for_superset()

    print("Logging in...")
    token = login()

    # --- Grocery ---
    print("\n=== Grocery ===")
    print("Setting up database connection...")
    gr_db_id = get_or_create_grocery_database(token)

    print("Setting up datasets...")
    gr_datasets = get_or_create_grocery_datasets(token, gr_db_id)

    if gr_datasets:
        print("Setting up charts...")
        gr_charts = get_or_create_grocery_charts(token, gr_datasets)

        print("Setting up dashboard...")
        gr_dash_id = get_or_create_grocery_dashboard(token, gr_charts)
        if gr_dash_id:
            link_charts_to_dashboard(gr_dash_id, list(gr_charts.values()))
    else:
        print("WARNING: no grocery datasets created, skipping charts/dashboard.")

    print("\nSetup complete.")


if __name__ == "__main__":
    main()
