#!/usr/bin/env python3
"""
Repair the bundled dashboard export: every pie / big_number chart carries the
SINGULAR `metric` its plugin reads.

WHY THIS EXISTS
---------------
`verisim_grocery_dashboards.zip` is a hand-exported Superset bundle. Its
`Stock_Aging_Breakdown_37.yaml` (viz_type `pie`, uuid a25b3b1f-…) shipped params
with the plural `metrics` and no singular `metric` — the exact shape the seed had
already been fixed to stop producing (t_1b933f2d, superset/
_superset_chart_params.py). A *later* import of the bundle re-creates that chart
as a NEW row on the bundle's uuid, and that copy is the one a fresh instance's
layout renders, while the seed's own chart (matched by name + dataset) is a
different row. So the tile said "Unexpected error" on a freshly installed
instance while every API and DB gate stayed green — proved live on 2026-09-21
(400 on the tile's own `orderby: [[null, false]]` request, 200 on the same chart
after the params were normalised).

The bundle is therefore the right place to fix it: Superset's dashboard importer
honours the chart params from the bundle, and install.sh imports with
`overwrite=true`, so a repaired bundle also heals instances that already carry
the broken copy (matched by chart uuid). Nothing to do at runtime.

SIBLINGS (run both after re-exporting a bundle)
-----------------------------------------------
  * `bind_dashboard_layout.py` — every layout CHART node bound to its chart uuid;
  * `repair_query_context.py`   — every chart carries a query_context;
  * `normalise_chart_metrics.py`— this script: the metric key those charts read.

Order does not matter: the three touch different keys of the same files, and
each asserts the others' territory is untouched.

WHAT IT WRITES
--------------
For each `charts/*.yaml` whose viz_type is pie / big_number / big_number_total
and whose params need it, `params.metrics` is hoisted into the singular
`params.metric` (keeping the key's position, so the bundle diff is one key) and
the plural key is dropped — the same rule, from the same module, that
`superset/create_grocery_ops_dashboard.py` applies to everything it seeds. The
chart's `query_context` is then rebuilt from the normalised params with
`repair_query_context.query_context_for()` — the seed helper's own rule — so the
stored query context and the params cannot disagree. In practice the rebuilt
value is byte-identical (the helper reads either key), which the script reports
rather than hides: `query_context unchanged` vs `query_context rebuilt`.

IDEMPOTENT. A second run writes nothing and reports `nothing to do`.

Usage:
    python3 superset/dashboards/normalise_chart_metrics.py [--check] [ZIP]

    --check   report only, change nothing; exit 1 if any pie / big_number chart
              lacks the singular metric (usable as a CI assertion).
"""

import argparse
import copy
import json
import os
import re
import sys
import zipfile

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
SUPERSET_DIR = os.path.dirname(HERE)
DEFAULT_ZIP = os.path.join(HERE, "verisim_grocery_dashboards.zip")

sys.path.insert(0, SUPERSET_DIR)
from _superset_chart_params import SINGULAR_METRIC_VIZ, normalise_metric_params  # noqa: E402

sys.path.insert(0, HERE)
import repair_query_context as rqc  # noqa: E402

# `params:` runs to the next top-level key (the export writes `query_context:`
# next). The block is re-dumped in the same style the export uses.
PARAMS_RE = re.compile(r"(?m)^params:$")
NEXT_TOP_KEY_RE = re.compile(r"(?m)^[a-z_]+:")
# Top-level keys this repair is allowed to touch at all.
TOUCHED = ("params", "query_context")


def dump_params(params):
    """The `params:` block, dumped exactly as the export writes it."""
    return yaml.safe_dump(
        {"params": params},
        default_flow_style=False,
        sort_keys=False,
        width=10**9,
        allow_unicode=True,
    ).rstrip("\n")


def splice_params(text, params):
    """Replace only the document's `params:` block; leave every other byte."""
    m = PARAMS_RE.search(text)
    if not m:
        raise SystemExit("no `params:` block found")
    rest = text[m.end():]
    nxt = NEXT_TOP_KEY_RE.search(rest)
    if not nxt:
        raise SystemExit("no top-level key after `params:`")
    head, tail = text[: m.start()], rest[nxt.start():]
    return head + dump_params(params) + "\n" + tail


def normalise_chart(text, dttm_by_uuid):
    """
    Return (new_text, report). new_text is None when the file needs no change.
    """
    doc = yaml.safe_load(text)
    name = doc.get("slice_name") or "?"
    viz_type = doc.get("viz_type")
    params = doc.get("params") or {}
    report = {"slice_name": name, "viz_type": viz_type, "action": "ok", "detail": ""}

    if viz_type not in SINGULAR_METRIC_VIZ:
        report["action"] = "n/a"
        report["detail"] = "reads the plural metrics — not this rule's family"
        return None, report

    plural = params.get("metrics")
    if params.get("metric") is not None and plural is None:
        label = params["metric"].get("label") if isinstance(params["metric"], dict) else None
        report["detail"] = "already singular metric: %s" % (label or params["metric"])
        return None, report

    normalised = normalise_metric_params(viz_type, copy.deepcopy(params), name)
    metric = normalised["metric"]
    label = metric.get("label") if isinstance(metric, dict) else metric
    if isinstance(plural, list):
        detail = "params.metrics (%d) -> params.metric: %s" % (len(plural), label)
    elif plural is not None:
        detail = "params.metrics (mapping) -> params.metric: %s" % label
    else:
        detail = "params.metric added: %s" % label

    new_text = splice_params(text, normalised)

    # The stored query_context is derived from the params: rebuild it with the
    # seed's own rule and keep the line only if it already says the same thing.
    qc_json, _, _ = rqc.query_context_for(
        {"params": normalised, "dataset_uuid": doc.get("dataset_uuid"), "slice_name": name},
        dttm_by_uuid,
    )
    existing_qc = doc.get("query_context")
    if existing_qc in (None, "", "null"):
        new_text = rqc.write_query_context_line(new_text, qc_json, name)
        detail += "; query_context built"
    else:
        try:
            same = json.loads(str(existing_qc)) == json.loads(qc_json)
        except (TypeError, ValueError):
            raise SystemExit("chart %r: query_context is not JSON — refusing to write" % name)
        if same:
            detail += "; query_context unchanged"
        else:
            new_text = rqc.write_query_context_line(new_text, qc_json, name)
            detail += "; query_context rebuilt"

    # The bundle is a contract with the importer: assert we changed only what we
    # meant to, and that the result is what a re-run would leave behind.
    back = yaml.safe_load(new_text)
    for key in set(doc) | set(back):
        if key in TOUCHED:
            continue
        if back.get(key) != doc.get(key):
            raise SystemExit("chart %r: %s changed — refusing to write" % (name, key))
    if back.get("params") != normalised:
        raise SystemExit("chart %r: params did not round-trip" % name)
    if "metrics" in back["params"]:
        raise SystemExit("chart %r: plural metrics survived the rewrite" % name)
    if back["params"].get("metric") is None:
        raise SystemExit("chart %r: singular metric missing after the rewrite" % name)
    if back.get("query_context") != qc_json:
        raise SystemExit("chart %r: query_context did not round-trip" % name)
    json.loads(back["query_context"])
    again = normalise_metric_params(viz_type, copy.deepcopy(back["params"]), name)
    if again != back["params"]:
        raise SystemExit("chart %r: not idempotent — a second pass would change params" % name)

    report.update(action="needs-repair", detail=detail)
    return new_text, report


def main():
    ap = argparse.ArgumentParser(
        description="Normalise the bundled export's pie / big_number chart params."
    )
    ap.add_argument("zip", nargs="?", default=DEFAULT_ZIP, help="bundle to repair (default: %(default)s)")
    ap.add_argument("--check", action="store_true", help="report only; exit 1 if any chart needs the singular metric")
    args = ap.parse_args()

    if not os.path.isfile(args.zip):
        raise SystemExit("no such bundle: %s" % args.zip)

    entries = rqc.read_entries(args.zip)
    dttm_by_uuid = rqc.dataset_dttm_cols(entries)
    charts = [(i, d) for i, d in entries if "/charts/" in i.filename and i.filename.endswith(".yaml")]

    replacements = {}
    reports = []
    for info, data in charts:
        new_text, report = normalise_chart(data.decode("utf-8"), dttm_by_uuid)
        report["file"] = os.path.basename(info.filename)
        reports.append(report)
        if new_text is not None:
            replacements[info.filename] = new_text.encode("utf-8")

    reports.sort(key=lambda r: r["file"])
    for r in reports:
        print("  %-13s %-42s %s" % (r["action"], r["slice_name"], r["detail"]))

    need = sum(1 for r in reports if r["action"] == "needs-repair")
    family = sum(1 for r in reports if r["action"] in ("needs-repair", "ok"))
    print(
        "\n%s: %d charts — %d in the singular-metric family, %d need repair"
        % (os.path.basename(args.zip), len(reports), family, need)
    )

    if args.check:
        if need:
            print("FAIL: %d pie/big_number chart(s) ship without the singular metric" % need)
            return 1
        print("OK: every pie/big_number chart carries the singular metric")
        return 0

    if not replacements:
        print("nothing to do — bundle already normalised")
        return 0

    before_names = [i.filename for i, _ in entries]
    rqc.write_entries(args.zip, entries, replacements)

    # Prove it landed: read the zip back and re-run the rule over what is on disk.
    after = rqc.read_entries(args.zip)
    if [i.filename for i, _ in after] != before_names:
        raise SystemExit("archive layout changed — refusing to keep the rewrite")
    left = []
    for info, data in after:
        if "/charts/" not in info.filename or not info.filename.endswith(".yaml"):
            continue
        doc = yaml.safe_load(data.decode("utf-8"))
        if doc.get("viz_type") not in SINGULAR_METRIC_VIZ:
            continue
        if (doc.get("params") or {}).get("metric") is None or "metrics" in (doc.get("params") or {}):
            left.append(info.filename)
    if left:
        raise SystemExit("still not normalised after the write: %s" % ", ".join(left))

    print("wrote %s — %d chart(s) normalised, %d chart(s) already were"
          % (args.zip, len(replacements), family - len(replacements)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
