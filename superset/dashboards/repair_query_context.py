#!/usr/bin/env python3
"""
Repair the bundled dashboard export: give every chart a real query_context.

WHY THIS EXISTS
---------------
`verisim_grocery_dashboards.zip` is a Superset dashboard export. It was taken
from an instance where these charts had never been (re)saved from the Explore
UI, so every chart file in it carries `query_context: null`.

That null is not cosmetic. Superset keeps the *compiled query* next to the chart
params; a chart without one renders

    "Chart has no query context saved. Please save the chart again."

and the tile never loads. The deploy measures exactly that: install.sh's
`verify_superset()` counts `slices where query_context is null` and reports the
install incomplete (`provisioned=no`) on any hit, and the acceptance gate fails
with "18 charts missing query_context" (e2e-testing/full-cycle.sh phase 8).

WHY THE BUNDLE IS THE RIGHT PLACE TO FIX IT
-------------------------------------------
Superset's importer honours `query_context` from the bundle: `ImportV1ChartSchema`
declares it (validated as JSON) and `import_chart()` -> `Slice.import_from_dict()`
persists it. And `update_chart_config_dataset()` rewrites the datasource id
embedded in the query_context to the *importing* instance's dataset id, so the
value baked into the bundle is not tied to the instance it was exported from.
(It rewrites `params.datasource`, `query_context.datasource` and every
`queries[].datasource` the same way.)

So repairing the export is a one-off, deterministic, offline operation that fixes
every instance at import time — and since install.sh imports with
`overwrite=true`, re-importing the repaired bundle also heals instances that are
already broken, matched by chart uuid. No meta-DB surgery, no runtime workaround.

WHAT IT WRITES
--------------
For each `charts/*.yaml` the existing `query_context: ...` line is replaced with
a query_context built from that chart's own `params` by the same helper the
scripted seed uses (`superset/_superset_query_context.py`) — so the bundle's
charts end up shaped exactly like the ones `superset/setup.py` POSTs. Nothing
else in the file is touched: the diff is one line per chart.

The temporal column is resolved the way that helper resolves it at seed time: the
chart's own `granularity_sqla` when it has one, otherwise the owning dataset's
`main_dttm_col` from the bundle's own `datasets/*.yaml`. Charts whose dataset has
no temporal column (`main_dttm_col: null`, e.g. mart_hourly_sales_pattern) get no
granularity — same as the seed produces for them.

The script is idempotent: a chart that already carries a query_context is left
alone. Run it again after re-exporting a bundle.

Usage:
    python3 superset/dashboards/repair_query_context.py [--check] [ZIP]

    --check   report only, change nothing; exit 1 if any chart still lacks a
              query_context (so it can be used as a CI assertion).
"""

import argparse
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
from _superset_query_context import build_query_context  # noqa: E402

QC_LINE_RE = re.compile(r"(?m)^query_context:.*$")
DATASOURCE_RE = re.compile(r"^(\d+)__")


def read_entries(zip_path):
    """[(ZipInfo, bytes)] in archive order — enough to rewrite the zip verbatim."""
    with zipfile.ZipFile(zip_path) as z:
        return [(info, z.read(info.filename)) for info in z.infolist()]


def write_entries(zip_path, entries, replacements):
    """Rewrite the archive, preserving order, names and per-entry metadata."""
    tmp = zip_path + ".tmp"
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        for info, data in entries:
            payload = replacements.get(info.filename, data)
            clone = zipfile.ZipInfo(info.filename, date_time=info.date_time)
            clone.compress_type = info.compress_type
            clone.external_attr = info.external_attr
            clone.internal_attr = info.internal_attr
            clone.create_system = info.create_system
            clone.comment = info.comment
            z.writestr(clone, payload)
    os.replace(tmp, zip_path)


def dataset_dttm_cols(entries):
    """dataset_uuid -> main_dttm_col, read from the bundle's own datasets/."""
    out = {}
    for info, data in entries:
        if "/datasets/" not in info.filename or not info.filename.endswith(".yaml"):
            continue
        doc = yaml.safe_load(data.decode()) or {}
        uuid = doc.get("uuid")
        if uuid:
            out[str(uuid)] = doc.get("main_dttm_col")
    return out


def build_chart_query_context(text, dttm_by_uuid):
    """
    Return (new_text, report) for a chart that needs repairing, or (None, report)
    when it already carries a query_context.
    """
    doc = yaml.safe_load(text)
    name = doc.get("slice_name") or "?"
    report = {"slice_name": name, "action": "ok", "detail": ""}

    existing = doc.get("query_context")
    if existing not in (None, "", "null"):
        report["detail"] = "already has a query_context (%d chars)" % len(str(existing))
        return None, report

    params = doc.get("params") or {}
    match = DATASOURCE_RE.match(str(params.get("datasource") or ""))
    if not match:
        raise SystemExit(
            "chart %r has no resolvable datasource in params (datasource=%r)"
            % (name, params.get("datasource"))
        )
    ds_id = int(match.group(1))

    # Mirror build_query_context()'s live behaviour: an explicit granularity in the
    # chart params wins and is copied through as-is; otherwise the dataset's
    # main_dttm_col is used, and only in that branch does it set `granularity` too.
    dttm = params.get("granularity_sqla") or params.get("granularity")
    from_dataset = False
    if not dttm:
        dttm = dttm_by_uuid.get(str(doc.get("dataset_uuid")))
        from_dataset = bool(dttm)

    qc_params = dict(params)
    if dttm:
        qc_params["granularity_sqla"] = dttm
    qc = json.loads(build_query_context(ds_id, qc_params))
    if from_dataset:
        qc["queries"][0]["granularity"] = dttm
        qc["queries"][0]["granularity_sqla"] = dttm

    qc_json = json.dumps(qc)
    line = yaml.safe_dump(
        {"query_context": qc_json},
        default_flow_style=False,
        sort_keys=False,
        width=10**9,
        allow_unicode=True,
    ).rstrip("\n")

    new_text, n = QC_LINE_RE.subn(lambda _m: line, text, count=1)
    if n != 1:
        raise SystemExit("chart %r: expected exactly one query_context line, found %d" % (name, n))

    # The bundle is the contract with the importer — assert we only changed what
    # we meant to, and that what we wrote round-trips as the JSON string Superset
    # will hand to Slice.import_from_dict after validate_json.
    back = yaml.safe_load(new_text)
    if back["params"] != doc["params"]:
        raise SystemExit("chart %r: params changed — refusing to write" % name)
    for key in ("uuid", "slice_name", "viz_type", "dataset_uuid"):
        if back.get(key) != doc.get(key):
            raise SystemExit("chart %r: %s changed — refusing to write" % (name, key))
    if back["query_context"] != qc_json:
        raise SystemExit("chart %r: query_context did not round-trip" % name)
    json.loads(back["query_context"])

    report.update(
        action="needs-repair",
        detail="datasource %d, granularity %s, %d chars"
        % (ds_id, dttm or "none", len(qc_json)),
    )
    return new_text, report


def main():
    ap = argparse.ArgumentParser(description="Repair the bundled dashboard export's chart query_context values.")
    ap.add_argument("zip", nargs="?", default=DEFAULT_ZIP, help="bundle to repair (default: %(default)s)")
    ap.add_argument("--check", action="store_true", help="report only; exit 1 if any chart lacks a query_context")
    args = ap.parse_args()

    if not os.path.isfile(args.zip):
        raise SystemExit("no such bundle: %s" % args.zip)

    entries = read_entries(args.zip)
    dttm_by_uuid = dataset_dttm_cols(entries)
    chart_names = [i.filename for i, _ in entries if "/charts/" in i.filename and i.filename.endswith(".yaml")]

    replacements = {}
    reports = []
    for info, data in entries:
        if "/charts/" not in info.filename or not info.filename.endswith(".yaml"):
            continue
        new_text, report = build_chart_query_context(data.decode("utf-8"), dttm_by_uuid)
        report["file"] = os.path.basename(info.filename)
        reports.append(report)
        if new_text is not None:
            replacements[info.filename] = new_text.encode("utf-8")

    reports.sort(key=lambda r: r["file"])
    for r in reports:
        print("  %-13s %-42s %s" % (r["action"], r["slice_name"], r["detail"]))

    need = sum(1 for r in reports if r["action"] == "needs-repair")
    ok = len(reports) - need
    print(
        "\n%s: %d charts — %d need repair, %d already carry a query_context"
        % (os.path.basename(args.zip), len(chart_names), need, ok)
    )

    if args.check:
        # Nothing was written, so "needs repair" is precisely the count of charts
        # the deploy's `slices where query_context is null` probe would find.
        if need:
            print("FAIL: %d chart(s) have no query_context" % need)
            return 1
        print("OK: every chart carries a query_context")
        return 0

    if not replacements:
        print("nothing to do — bundle already repaired")
        return 0

    # Rewrite, then prove it landed by reading the file back.
    write_entries(args.zip, entries, replacements)
    after = read_entries(args.zip)
    before_meta = [(i.filename, len(d)) for i, d in entries]
    after_meta = [(i.filename, len(d)) for i, d in after]
    if [n for n, _ in before_meta] != [n for n, _ in after_meta]:
        raise SystemExit("archive layout changed — refusing to keep the rewrite")

    distinct = set()
    for info, data in after:
        if "/charts/" not in info.filename or not info.filename.endswith(".yaml"):
            continue
        doc = yaml.safe_load(data.decode("utf-8"))
        if not doc.get("query_context"):
            raise SystemExit("%s: query_context missing after write" % info.filename)
        distinct.add(json.dumps(json.loads(doc["query_context"]), sort_keys=True))
    print("wrote %s — %d chart(s) now carry a query_context (%d distinct values)" % (args.zip, len(chart_names), len(distinct)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
