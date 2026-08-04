#!/usr/bin/env python3
"""
Shared Superset helper: build a query_context from chart params.

Import this in any seed script and call:
    qc_json = build_query_context(ds_id, params_dict)

Then include "query_context": qc_json in the chart POST payload.

Without this, Superset charts are created as "zombies" — they exist in the UI
but return "Chart has no query context saved. Please save the chart again."
when queried.

Background: Superset's REST API does NOT auto-generate query_context from params
on POST. The Explore UI generates it when you click "Save." For programmatic
chart creation, we must build it ourselves.
"""

import json
import copy


def build_query_context(ds_id, params, token=None, base_url="http://superset:8088"):
    """
    Build a query_context JSON string from chart params.

    Args:
        ds_id: integer dataset ID
        params: dict of chart params (typically from params_extra dict)
        token: optional Superset JWT. When provided AND params have no explicit
               granularity, the dataset's main_dttm_col is fetched and used so
               snapshot marts (which have no natural time axis) still resolve a
               valid temporal column. Without this, Superset 4.x returns
               "Datetime column not provided" for charts on static tables.
        base_url: Superset base URL (used only when token is provided)

    Returns:
        JSON string suitable for the 'query_context' field in chart POST/PUT.
    """
    raw_metrics = params.get("metrics") or params.get("metric") or []
    if not isinstance(raw_metrics, list):
        raw_metrics = [raw_metrics]

    # Convert adhoc_filters from params format (comparator) to
    # query_context format (val). Also strip expressionType/clause
    # which the query_context validator rejects.
    qc_filters = []
    for f in params.get("adhoc_filters", []):
        f2 = {}
        if "col" in f:
            f2["col"] = f["col"]
        elif "subject" in f:
            f2["col"] = f["subject"]
        if "op" in f:
            f2["op"] = f["op"]
        elif "operator" in f:
            f2["op"] = f["operator"]
        if "comparator" in f:
            f2["val"] = f["comparator"]
        elif "val" in f:
            f2["val"] = f["val"]
        if f2:
            qc_filters.append(f2)

    qc = {
        "datasource": {"id": ds_id, "type": "table"},
        "queries": [
            {
                "metrics": raw_metrics,
                "columns": params.get("groupby", []),
                "filters": qc_filters,
                "row_limit": params.get("row_limit") or params.get("rowLimit") or 10000,
                "time_range": params.get("time_range") or "No filter",
            }
        ],
        "force": True,
        "result_format": "json",
        "result_type": "full",
    }

    q = qc["queries"][0]
    for field in [
        "granularity_sqla",
        "granularity",
        "orderby",
        "timeseries_limit_metric",
    ]:
        if params.get(field):
            q[field] = params[field]
    if params.get("order_desc") is not None:
        q["order_desc"] = params["order_desc"]

    # Auto-resolve granularity from the dataset's main_dttm_col when the chart
    # params do not specify one. This is what makes snapshot marts (e.g.
    # mart_inventory_valuation, mart_fleet_cost) render without a
    # "Datetime column not provided" error after a clean reseed.
    if not q.get("granularity") and not q.get("granularity_sqla") and token:
        try:
            import requests

            r = requests.get(
                f"{base_url}/api/v1/dataset/{ds_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            if r.status_code == 200:
                dttm = r.json().get("result", {}).get("main_dttm_col")
                if dttm:
                    q["granularity"] = dttm
                    q["granularity_sqla"] = dttm
        except Exception:
            pass

    return json.dumps(qc)
