#!/usr/bin/env python3
"""
Create Data Quality dashboard in Superset via REST API.

Registers new mart datasets (transport, timeclock, fulfillment) and builds
a Data Quality & Operations monitoring dashboard.

Every dataset the charts bind to is resolved BY NAME (schema.table). Superset
dataset ids are instance-local: they depend on registration order, so they differ
between a fresh seed, dev and test. A hardcoded id map cannot work — it silently
bound 'Stock Aging' / 'Inventory Location Breakdown' / 'Avg Transaction Count by
Hour' to whatever table happened to hold that id, and the tiles rendered
"Columns missing in dataset" while every API still returned 200.

Two hard rules in here, both learned from that defect:
  1. No dataset id is ever written into this file or read from a live listing's
     heuristic (the old `id > 18` scrape). Resolve by (schema, table_name).
  2. Before a chart is built, assert the columns its params reference actually
     exist on the resolved dataset. A miss is a loud, non-zero failure — never a
     chart bound to the wrong table.

Usage:
  python3 create_data_quality_dashboard.py [--superset-url URL] [--username USER] [--password PASS]
"""

import json
import sys
import os
import random
import string
import time
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests


SUPERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin"

MART_SCHEMA = "mart"
DASH_TITLE = "Data Quality & Operations"
DASH_SLUG = "data-quality-ops"

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


def api(method, url, retries=5, **kwargs):
    """HTTP call with backoff on the API's rate limit.

    Superset answers bursts with `429 Too Many Requests: 50 per 1 second`. The
    prune pass issues two calls per duplicate chart, so a legacy instance with
    dozens of duplicates trips it and half the cleanup silently does not happen
    (found on dev: 3 of 45 duplicates survived). Retry instead of racing.
    """
    delay = 1.0
    resp = None
    for _ in range(max(retries, 1)):
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 429:
            return resp
        try:
            wait = float(resp.headers.get("Retry-After", delay))
        except (TypeError, ValueError):
            wait = delay
        time.sleep(min(max(wait, 0.5), 10.0))
    return resp


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


# Every chart this dashboard owns. `key` is the MART TABLE NAME it must bind to —
# resolved to a live dataset id by name at run time, one lookup per chart. `name`
# doubles as the chart's identity: an existing chart of the same name is rebound
# rather than duplicated.
SECTIONS = [
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

# The marts registered (if absent) before the charts are built.
NEW_MARTS = [
    ("mart_transport_load_summary", MART_SCHEMA),
    ("mart_fleet_utilization", MART_SCHEMA),
    ("mart_transport_daily_metrics", MART_SCHEMA),
    ("mart_attendance_summary", MART_SCHEMA),
    ("mart_daily_attendance_stats", MART_SCHEMA),
    ("mart_employee_hours_vs_schedule", MART_SCHEMA),
    ("mart_fulfillment_operations", MART_SCHEMA),
    ("mart_fulfillment_pick_accuracy", MART_SCHEMA),
    ("mart_daily_fulfillment_summary", MART_SCHEMA),
    ("mart_order_fulfillment_funnel", MART_SCHEMA),
]

# Snapshots/marts that need a main_dttm_col so Superset treats them as temporal.
# Independent of the chart list above: mart_fulfillment_operations carries no
# chart on this dashboard but other dashboards plot it over time.
DTTM_COLUMNS = [
    ("mart_transport_load_summary", "load_date"),
    ("mart_transport_daily_metrics", "load_date"),
    ("mart_attendance_summary", "event_date"),
    ("mart_daily_attendance_stats", "event_date"),
    ("mart_employee_hours_vs_schedule", "report_date"),
    ("mart_fulfillment_operations", "order_received_at"),
    ("mart_daily_fulfillment_summary", "report_date"),
    ("mart_order_fulfillment_funnel", "order_date"),
]


def _paged_list(token, base_url, endpoint, page_size=200):
    """Fetch every row of a list endpoint.

    The plain `page`/`page_size` query params are ignored by these endpoints (the
    RSION `q` form is the one the API honours), which is exactly how the old
    `id > 18` scrape ended up looking at an arbitrary slice of the table. Always
    page through to the end and return the whole list.
    """
    out = []
    page = 0
    while page < 50:
        r = requests.get(urljoin(base_url, endpoint),
            headers=headers(token),
            params={"q": json.dumps({"page": page, "page_size": page_size})},
            timeout=30)
        r.raise_for_status()
        body = r.json()
        rows = body.get("result", [])
        out.extend(rows)
        count = body.get("count")
        if not rows or (count is not None and len(out) >= count):
            break
        page += 1
    return out


def _find_dataset_id(token, base_url, db_id, table_name, schema=MART_SCHEMA, datasets=None):
    """Resolve a dataset id by (schema, table_name) — never by id.

    `datasets` lets a caller reuse one listing for many lookups.
    """
    if datasets is None:
        datasets = _paged_list(token, base_url, "/api/v1/dataset/")
    for d in datasets:
        if d.get("table_name") != table_name or d.get("schema") != schema:
            continue
        db = d.get("database")
        row_db_id = db.get("id") if isinstance(db, dict) else db
        if db_id and row_db_id and row_db_id != db_id:
            continue
        return d["id"]
    return None


def _dataset_columns(token, base_url, ds_id):
    """The column names the dataset actually exposes in Superset."""
    r = requests.get(urljoin(base_url, f"/api/v1/dataset/{ds_id}"),
                     headers=headers(token), timeout=20)
    r.raise_for_status()
    return {c.get("column_name")
            for c in r.json().get("result", {}).get("columns", [])}


def _chart_columns(params):
    """The dataset columns a chart's params actually reference.

    Derived from the params themselves rather than a hand-kept list, so a chart
    that grows a metric cannot drift away from its assertion.
    """
    cols = set()

    def add_metric(m):
        if isinstance(m, dict) and isinstance(m.get("column"), dict):
            name = m["column"].get("column_name")
            if name:
                cols.add(name)

    add_metric(params.get("metric"))
    for m in params.get("metrics") or []:
        add_metric(m)
    for g in params.get("groupby") or []:
        if isinstance(g, str):
            cols.add(g)
    for f in params.get("adhoc_filters") or []:
        if isinstance(f, dict):
            subject = f.get("subject") or f.get("col")
            if isinstance(subject, str) and subject and not subject.startswith("("):
                cols.add(subject)
    return cols


def _list_charts(token, base_url):
    return _paged_list(token, base_url, "/api/v1/chart/")


def _charts_by_name(token, base_url):
    """{slice_name: [chart ids sorted asc]} — the lowest id is the canonical one."""
    by_name = {}
    for c in _list_charts(token, base_url):
        name = c.get("slice_name")
        if name:
            by_name.setdefault(name, []).append(c["id"])
    for name in by_name:
        by_name[name].sort()
    return by_name


def _find_dashboard_id(token, base_url, slug):
    """Resolve a dashboard id by slug (None when it does not exist yet)."""
    try:
        r = requests.get(urljoin(base_url, "/api/v1/dashboard/"),
            headers=headers(token),
            params={"q": f'(page:0,page_size:50,filters:!((col:slug,opr:eq,value:{slug})))'},
            timeout=10)
        if r.status_code == 200:
            for d in r.json().get("result", []):
                if d.get("slug") == slug:
                    return d["id"]
    except Exception:
        pass
    return None


def _verify_bindings(token, base_url, dash_id, canonical_by_name):
    """Post-condition check: the dashboard is bound to exactly the canonical charts.

    Two failure modes the API never reports: a chart that was not linked renders
    "no chart definition associated with this component", and a leftover
    duplicate renders its own (stale) binding. Both are relationships, so the
    seed reads back the relationship it just wrote.
    """
    problems = []
    by_name = _charts_by_name(token, base_url)
    for name, cid in sorted(canonical_by_name.items()):
        linked = _linked_dashboard_ids(token, base_url, cid)
        if dash_id not in linked:
            problems.append(f"chart '{name}' (id={cid}) is not linked to dashboard {dash_id}")
        for extra in by_name.get(name, []):
            if extra == cid:
                continue
            if dash_id in _linked_dashboard_ids(token, base_url, extra):
                problems.append(f"duplicate chart '{name}' (id={extra}) is still linked to dashboard {dash_id}")
    return problems


def _linked_dashboard_ids(token, base_url, cid):
    r = api("GET", urljoin(base_url, f"/api/v1/chart/{cid}"),
            headers=headers(token), timeout=20)
    if r.status_code != 200:
        return []
    return [d["id"] for d in r.json().get("result", {}).get("dashboards", [])
            if isinstance(d, dict) and d.get("id")]


def _prune_duplicate_chart(token, base_url, cid, dash_id):
    """Remove a duplicate chart, but only when it belongs to this dashboard alone.

    Superset refuses to delete a chart a dashboard still references, so the link
    is dropped first. A chart that is also used elsewhere is left in place (it
    cannot render on this dashboard once the layout stops referencing it).
    """
    try:
        r = api("GET", urljoin(base_url, f"/api/v1/chart/{cid}"),
                headers=headers(token), timeout=20)
        dash_ids = []
        if r.status_code == 200:
            dash_ids = [d["id"] for d in r.json().get("result", {}).get("dashboards", [])
                        if isinstance(d, dict) and d.get("id") != dash_id]
        api("PUT", urljoin(base_url, f"/api/v1/chart/{cid}"),
            headers=headers(token), json={"dashboards": dash_ids}, timeout=20)
        if dash_ids:
            print(f"  ~ Duplicate chart id={cid} kept (also on dashboards {dash_ids}); unlinked from {dash_id}")
            return False
        resp = api("DELETE", urljoin(base_url, f"/api/v1/chart/{cid}"),
                   headers=headers(token), timeout=20)
        if resp.status_code in (200, 204):
            print(f"  ✓ Pruned duplicate chart id={cid}")
            return True
        print(f"  ⚠ Could not delete duplicate chart id={cid}: "
              f"{resp.status_code} {resp.text[:120]}")
    except Exception as e:
        print(f"  ⚠ Could not delete duplicate chart id={cid}: {e}")
    return False


def register_dataset(token, base_url, db_id, table_name, schema):
    """Register an existing table as a Superset dataset (idempotent).

    A 422 means it is already registered — resolve it by name so callers always
    get a usable id back.
    """
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
        ds_id = _find_dataset_id(token, base_url, db_id, table_name, schema)
        if ds_id:
            print(f"  ~ '{schema}.{table_name}' already exists (id={ds_id})")
            return ds_id
        print(f"  ✗ '{schema}.{table_name}' exists per the API but could not be resolved by name")
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


def refresh_dataset(token, base_url, ds_id, table_name=None):
    """Re-sync a dataset's column metadata from the physical table.

    Thin wrapper over the shared helper (`_superset_dataset_metadata`), which is
    what setup.py, create_missing_dashboards.py and create_grocery_ops_dashboard.py
    call too — one implementation of the endpoint call, one place to fix it.

    A dataset row is created once; a mart that later gains a column keeps the old
    column list until it is refreshed. That is not cosmetic: the seeds set
    main_dttm_col to a column that only exists after the dbt rebuild (the
    snapshot `as_of_date` on mart_hourly_sales_pattern), and with stale metadata
    every chart on that dataset 400s with an unresolvable column — visible only
    in the browser, never in an API 200.
    """
    from _superset_dataset_metadata import refresh_dataset as _refresh
    return _refresh(token, base_url, ds_id, table_name, MART_SCHEMA)


def create_chart(token, base_url, ds_id, slice_name, viz_type, params_extra,
                 existing_id=None):
    """Create a Superset chart, or update it in place when one already exists.

    The update path is what heals an instance seeded by the old id-based script:
    it rewrites datasource_id, params and query_context, so a reused chart can
    never keep pointing at the wrong mart.
    """
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
    body = {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(base_params),
        "query_context": build_query_context(ds_id, base_params, token, base_url),
    }
    if existing_id:
        resp = requests.put(urljoin(base_url, f"/api/v1/chart/{existing_id}"),
            headers=headers(token), json=body, timeout=30)
        if resp.status_code == 200:
            print(f"  ✓ Chart '{slice_name}' rebound to dataset {ds_id} (id={existing_id})")
            return {"id": existing_id, "slice_name": slice_name}
        print(f"  ✗ Failed to update '{slice_name}' (id={existing_id}): "
              f"{resp.status_code} {resp.text[:200]}")
        return None
    resp = requests.post(urljoin(base_url, "/api/v1/chart/"),
        headers=headers(token),
        json=dict(body, dashboards=[]), timeout=30)
    if resp.status_code == 201:
        result = resp.json()
        cid = result["id"]
        print(f"  ✓ Chart '{slice_name}' (id={cid}, dataset={ds_id})")
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

    # Step 1: Register new mart datasets, then resolve EVERY dataset the charts
    # need by name. Dataset ids are instance-local — resolving by name is the
    # only binding that survives a reseed, a different slot, or a new dataset
    # registered ahead of these.
    print("--- Step 1: Register New Mart Datasets ---")
    for table, schema in NEW_MARTS:
        register_dataset(token, args.superset_url, grocery_db_id, table, schema)

    print("\n--- Step 1b: Resolve datasets by name ---")
    datasets = _paged_list(token, args.superset_url, "/api/v1/dataset/")
    resolved = {}
    for table in sorted({sec["key"] for sec in SECTIONS}):
        ds_id = _find_dataset_id(token, args.superset_url, grocery_db_id, table,
                                 MART_SCHEMA, datasets)
        if ds_id is None:
            print(f"  ✗ '{MART_SCHEMA}.{table}' is not registered in Superset — "
                  f"run the dbt mart build, then re-seed")
        else:
            print(f"  ✓ {MART_SCHEMA}.{table} -> dataset {ds_id}")
            resolved[table] = ds_id
    if len(resolved) != len({sec["key"] for sec in SECTIONS}):
        print("\n✗ ABORT: a chart's dataset is missing (see above). Refusing to "
              "build charts against the wrong table.")
        sys.exit(1)
    print()

    # Step 1c: also resolve the tables that only need a date column, then refresh
    # every target's column metadata from the EDW. Registration happens once per
    # instance; marts keep growing columns afterwards, and a dataset whose column
    # list is stale 400s any chart that touches a new column.
    print("--- Step 1c: Refresh Dataset Column Metadata ---")
    for table, _col in DTTM_COLUMNS:
        if table in resolved:
            continue
        ds_id = _find_dataset_id(token, args.superset_url, grocery_db_id, table,
                                 MART_SCHEMA, datasets)
        if ds_id:
            resolved[table] = ds_id
            print(f"  ✓ {MART_SCHEMA}.{table} -> dataset {ds_id} (date column only)")
    for table in sorted(resolved):
        refresh_dataset(token, args.superset_url, resolved[table], table)
    print()

    # Step 2: Set date columns on new datasets (by resolved name, never by id)
    print("--- Step 2: Configure Date Columns ---")
    for table, col in DTTM_COLUMNS:
        if resolved.get(table):
            set_main_dttm(token, args.superset_url, resolved[table], col)
        else:
            print(f"  ⚠ '{MART_SCHEMA}.{table}' not registered — "
                  f"cannot set main_dttm_col='{col}'")
    print()

    # Step 3: Create (or rebind) charts
    print("--- Step 3: Create Charts ---")
    charts_by_name = _charts_by_name(token, args.superset_url)
    canonical_names = {sec["name"] for sec in SECTIONS}
    charlist = []
    failures = []
    for sec in SECTIONS:
        table = sec["key"]
        ds_id = resolved[table]
        wanted = _chart_columns(sec["params"])
        have = _dataset_columns(token, args.superset_url, ds_id)
        missing = sorted(wanted - have)
        if missing:
            print(f"  ✗ '{sec['name']}': dataset {ds_id} ({table}) is missing "
                  f"{missing} — refusing to build the chart")
            failures.append(sec["name"])
            continue
        existing = charts_by_name.get(sec["name"], [])
        c = create_chart(token, args.superset_url, ds_id, sec["name"], sec["viz"],
                         sec["params"], existing_id=existing[0] if existing else None)
        if c:
            charlist.append(c)
    if failures or len(charlist) != len(SECTIONS):
        print(f"\n✗ ABORT: {len(failures) or (len(SECTIONS) - len(charlist))} chart(s) could not "
              f"be built ({', '.join(failures) if failures else 'creation failed'})")
        sys.exit(1)
    print()

    # Step 3b: prune the same-name duplicates earlier id-based seeds left behind.
    # They are the 21 broken tiles on dashboard 12: every re-run appended three
    # charts bound to the wrong mart, and 'dashboard_slices' kept them all.
    print("--- Step 3b: Prune Duplicate Charts ---")
    dash_id = _find_dashboard_id(token, args.superset_url, DASH_SLUG)
    pruned = 0
    duplicates = 0
    for name in sorted(canonical_names):
        ids = charts_by_name.get(name, [])
        for extra in ids[1:]:
            duplicates += 1
            if _prune_duplicate_chart(token, args.superset_url, extra, dash_id):
                pruned += 1
    print(f"  ✓ {pruned} of {duplicates} duplicate chart(s) pruned\n")

    # Step 4: Create dashboard
    print("--- Step 4: Create Dashboard ---")
    if charlist:
        create_dashboard(token, args.superset_url, charlist, DASH_TITLE, DASH_SLUG)
    else:
        print("  ✗ No charts created")
        sys.exit(1)

    # Step 4b: the post-condition. A chart that never got linked renders
    # "no chart definition", a leftover duplicate renders its stale binding —
    # neither is visible to an API status code, so assert the state directly.
    print("\n--- Step 4b: Verify Dashboard Bindings ---")
    dash_id = _find_dashboard_id(token, args.superset_url, DASH_SLUG)
    if not dash_id:
        print(f"  ✗ Dashboard '{DASH_SLUG}' not found after the seed")
        sys.exit(1)
    problems = _verify_bindings(token, args.superset_url, dash_id,
                                {c["slice_name"]: c["id"] for c in charlist})
    if problems:
        for p in problems:
            print(f"  ✗ {p}")
        sys.exit(1)
    print(f"  ✓ dashboard {dash_id}: {len(charlist)} charts linked, no duplicate bound")

    # Step 5: report the binding this run produced, so the log is auditable
    # without opening the UI.
    print("\n--- Chart bindings ---")
    for sec in SECTIONS:
        print(f"  {sec['name']:<30} -> {MART_SCHEMA}.{sec['key']} (dataset {resolved[sec['key']]})")
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
