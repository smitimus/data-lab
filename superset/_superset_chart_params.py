#!/usr/bin/env python3
"""
Shared Superset helper: the SINGULAR-metric rule for pie / big_number charts.

Import this in any seed script that builds a chart payload and call:

    normalise_metric_params(viz_type, params, slice_name)

`params` is normalised in place (exactly one non-null `metric`, plural `metrics`
dropped). It returns the same dict, so it can be used inline.

WHY THE RULE EXISTS
-------------------
Charts whose frontend plugin reads the SINGULAR `params.metric` — pie,
big_number and big_number_total — send an `orderby` built from it. The Echarts
pie plugin, for instance, builds

    orderby: [[metric, false]]        (only when sort_by_metric is set)

so a chart whose params carry the plural `metrics` (and therefore have no
`metric` at all) sends `orderby: [[null, false]]`, which the
/api/v1/chart/data validator rejects with

    Request is incorrect:
    {'queries': {0: {'orderby': {0: {0: ['Field may not be null.']}}}}}

and the tile renders "Unexpected error" forever — even though the chart's stored
query_context is perfectly valid. That asymmetry is why every API and DB gate
stays green while the browser shows a broken tile (proved live on 2026-09-21:
t_1b933f2d, then the bundle copy in t_0f50c0ab).

The rule is not cosmetic, and it is not "one of the accepted shapes": a chart in
this family whose params survive without a singular `metric` can never render.

WHERE IT IS ENFORCED
--------------------
  * `superset/create_grocery_ops_dashboard.py` — every chart it seeds/PUTs;
  * `superset/dashboards/normalise_chart_metrics.py` — the offline repair of the
    bundled export, so a fresh import cannot re-create the shape;
  * `e2e-testing/test-install-dashboards.sh` — the regression gate over the
    bundle actually shipped in the repo.

A chart in this family with no metric at all is a definition error, not a data
condition: normalising raises ValueError rather than shipping a tile that can
never render.
"""

SINGULAR_METRIC_VIZ = ("pie", "big_number", "big_number_total")


def normalise_metric_params(viz_type, params, slice_name="?"):
    """
    Normalise `params` for the viz types that read the singular `metric`.

    Returns `params` (mutated in place). Chart families the rule does not cover
    are returned untouched: dist_bar / echarts_* / table / heatmap read the
    plural `metrics`, and rewriting them would break them.

    Raises ValueError when the chart family has no metric at all to hoist.
    """
    if viz_type not in SINGULAR_METRIC_VIZ:
        return params

    plural = params.get("metrics")
    singular = params.get("metric")
    if singular is None and plural is not None:
        if isinstance(plural, list) and len(plural) > 0:
            singular = plural[0]
        elif isinstance(plural, dict):
            singular = plural
    if singular is None:
        # A definition error in CHARTS, not a data condition: seeding this would
        # produce a tile that can never render.
        raise ValueError(
            f"{slice_name}: {viz_type} chart has no metric — give it "
            "params['metrics'] (or params['metric']) so it can render"
        )

    if plural is not None:
        if params.get("metric") is not None:
            # Already singular: the plural key is the only thing to drop.
            rebuilt = [(key, value) for key, value in params.items() if key != "metrics"]
        else:
            # Hoist in place: the singular key takes the POSITION the plural key
            # had. The seed's dict order is immaterial to Superset, but the
            # bundled yaml is reviewed as a diff, and this keeps that diff to one
            # key instead of a block moved to the end of `params`.
            rebuilt = []
            for key, value in params.items():
                if key == "metrics":
                    rebuilt.append(("metric", singular))
                elif key == "metric":
                    continue  # the null placeholder the hoist replaces
                else:
                    rebuilt.append((key, value))
        params.clear()
        params.update(rebuilt)
    return params
