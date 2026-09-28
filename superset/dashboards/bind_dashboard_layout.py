#!/usr/bin/env python3
"""
Repair the bundled dashboard export: bind every layout slot to the chart it names.

WHY THIS EXISTS
---------------
A Superset dashboard stores its grid twice: `position_json` (the tiles, each
CHART node carrying `meta.chartId` = a *slice id*) and `dashboard_slices` (the
chart -> dashboard links the frontend hydrates from). Slice ids are per
instance; the export carries them frozen from the instance it was taken on.

Superset's importer knows that and rebinds the ids — but only for the CHART
nodes it can identify, and it identifies them **by uuid**:

    superset/commands/dashboard/importers/v1/utils.py
        build_uuid_to_id_map()   # {meta.uuid: meta.chartId} for CHART nodes with a uuid
        update_id_refs()         # child["meta"]["chartId"] = chart_ids[child["meta"]["uuid"]]

    superset/commands/dashboard/importers/v1/__init__.py
        find_chart_uuids(position)          # which charts this dashboard needs
        ... chart_ids[chart.uuid] = chart.id
        for uuid in find_chart_uuids(config["position"]):   # and the dashboard_slices links

So a CHART node without `meta.uuid` keeps the *exporting* instance's chartId,
and a chart that no CHART node names is never imported at all. On this bundle
that produced, verbatim:

  * `Gas_Station_Overview_3.yaml` — 7 CHART nodes, none with a uuid, naming
    charts (Total Revenue, POS vs Fuel Revenue, Fuel Sales by Grade, ...) that
    the bundle does not ship at all -> 7 tiles "There is no chart definition
    associated with this component".
  * `Grocery_Overview_4.yaml` / `Grocery_Operations_5.yaml` — two generations of
    CHART nodes side by side: 7/10 legacy nodes without a uuid (chartIds 17-24 /
    37-46, the exporting instance's) next to the current 8/10 nodes that *do*
    carry a uuid. The uuid-keyed generation imported correctly; the legacy one
    kept the exporting instance's ids and resolved to whatever chart now holds
    that id (on the dev/test slots: another dashboard's chart, hence broken
    tiles, and on Grocery Overview 18 tiles rendered where 8 were intended).

WHAT IT WRITES
--------------
For every `dashboards/*.yaml` in the bundle:

  1. each CHART node's `meta.uuid` is set to the uuid of the chart the node
     names — resolved through the node's `chartId`, i.e. the numeric suffix of
     the bundled `charts/<slug>_<chartId>.yaml` (the export names every chart
     file that way, and every legacy node's chartId refers to it). An existing
     `meta.uuid` is asserted to agree; a node whose `meta.sliceName` disagrees
     with that chart's `slice_name` is an error, not a silent rebind.
  2. where two CHART nodes name the same chart (the legacy + current
     generations), the uuid-keyed one survives and the others are dropped; rows
     left empty are removed from the grid, so a dashboard renders each of its
     charts once.
  3. dashboards listed in `retired_dashboards.txt` are dropped from the bundle
     entirely (see that file: a layout whose charts no longer exist anywhere, or
     a dashboard a seed script already builds).
  4. the archive is then pruned to the CLOSURE of the dashboards it still ships:
     a chart no shipped layout names is dropped, a dataset no shipped chart (and
     no native filter) points at is dropped, a database no shipped dataset points
     at is dropped. Superset's dashboard import only imports a chart a layout
     names, so an orphan chart is never imported — and install.sh's
     `check_zip_landed()` asserts that EVERY bundled object landed, so an orphan
     in the archive fails `install.sh --dashboards-only` with "only 10/18 charts
     present". Dropping a dashboard therefore has to take its now-orphan charts
     (and their datasets) with it; that is what step 4 does.

Nothing else changes: the `position:` block is the only part of a dashboard file
that is rewritten, and it round-trips through yaml before it is written.

IDEMPOTENT. Re-running writes nothing once the bundle is bound, and because
install.sh imports with `overwrite=true`, re-importing a bound bundle also heals
instances that already carry the stale layout (matched by the chart uuids the
nodes now carry). A *retired* dashboard needs one extra step on such an
instance — the import is additive and never deletes — which is what install.sh's
`retire_dashboards()` does from the same `retired_dashboards.txt`.

Usage:
    python3 superset/dashboards/bind_dashboard_layout.py [--check] [--retired FILE] [ZIP]

    --retired  the retired-dashboard list to read (default: the one next to this
               script; a test points it at a fixture)
    --check   report only, change nothing; exit 1 if any CHART node is unbound,
              duplicated, names a chart the bundle does not ship, if a retired
              dashboard is still in the bundle, or if the archive ships anything
              no dashboard needs (usable as a CI assertion).
"""

import argparse
import copy
import os
import re
import sys
import zipfile

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ZIP = os.path.join(HERE, "verisim_grocery_dashboards.zip")
RETIRED_FILE = os.path.join(HERE, "retired_dashboards.txt")

CHART_FILE_RE = re.compile(r"_(\d+)\.yaml$")
# `position:` runs to the next top-level key (Superset exports `metadata:` next).
POSITION_RE = re.compile(r"(?ms)^position:$")


def read_entries(zip_path):
    """[(ZipInfo, bytes)] in archive order — enough to rewrite the zip verbatim."""
    with zipfile.ZipFile(zip_path) as z:
        return [(info, z.read(info.filename)) for info in z.infolist()]


def write_entries(zip_path, entries, replacements, dropped):
    """Rewrite the archive, preserving order, names and per-entry metadata."""
    tmp = zip_path + ".tmp"
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        for info, data in entries:
            if info.filename in dropped:
                continue
            payload = replacements.get(info.filename, data)
            clone = zipfile.ZipInfo(info.filename, date_time=info.date_time)
            clone.compress_type = info.compress_type
            clone.external_attr = info.external_attr
            clone.internal_attr = info.internal_attr
            clone.create_system = info.create_system
            clone.comment = info.comment
            z.writestr(clone, payload)
    os.replace(tmp, zip_path)


def read_retired(path=RETIRED_FILE):
    """uuid -> title, from `retired_dashboards.txt` (blank/# lines ignored)."""
    out = {}
    if not os.path.isfile(path):
        return out
    with open(path) as fh:
        for line in fh:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split()
            out[parts[0].lower()] = " ".join(parts[1:]) or "(untitled)"
    return out


def chart_index(entries):
    """chartId (int) -> {uuid, slice_name, file}, from the bundle's charts/."""
    out = {}
    for info, data in entries:
        if "/charts/" not in info.filename or not info.filename.endswith(".yaml"):
            continue
        m = CHART_FILE_RE.search(info.filename)
        if not m:
            raise SystemExit("chart file %s has no _<id>.yaml suffix" % info.filename)
        doc = yaml.safe_load(data.decode("utf-8")) or {}
        out[int(m.group(1))] = {
            "uuid": str(doc.get("uuid") or ""),
            "slice_name": doc.get("slice_name"),
            "file": os.path.basename(info.filename),
        }
    return out


def dump_position(position):
    """The `position:` block, dumped exactly as the export writes it."""
    return yaml.safe_dump(
        {"position": position},
        default_flow_style=False,
        sort_keys=False,
        width=10**9,
        allow_unicode=True,
    ).rstrip("\n")


def splice_position(text, position):
    """Replace only the document's `position:` block; leave every other byte."""
    m = POSITION_RE.search(text)
    if not m:
        raise SystemExit("no `position:` block found")
    rest = text[m.end():]
    nxt = re.search(r"(?m)^[a-z_]+:", rest)
    if not nxt:
        raise SystemExit("no top-level key after `position:`")
    head, tail = text[: m.start()], rest[nxt.start():]
    return head + dump_position(position) + "\n" + tail


def rebind_dashboard(text, charts, name):
    """
    Return (new_text, report). new_text is None when the file is already bound.
    """
    doc = yaml.safe_load(text)
    position = doc.get("position")
    if not isinstance(position, dict):
        raise SystemExit("%s: position is not a mapping" % name)
    original = copy.deepcopy(position)

    nodes = {
        k: v
        for k, v in position.items()
        if isinstance(v, dict) and v.get("type") == "CHART"
    }
    before = len(nodes)

    reports = []
    # 1. bind every CHART node to the chart it names.
    for key, node in nodes.items():
        meta = node.get("meta")
        if not isinstance(meta, dict):
            raise SystemExit("%s: %s has no meta" % (name, key))
        cid = meta.get("chartId")
        cid_str = "" if cid is None else str(cid).strip()
        chart = charts.get(int(cid_str)) if cid_str.isdigit() else None
        if chart is None:
            raise SystemExit(
                "%s: %s names chartId %r, which the bundle does not ship "
                "(its layout is dangling — the importer would leave the tile "
                "pointing at whatever chart holds that id on the target)"
                % (name, key, cid)
            )
        if not chart["uuid"]:
            raise SystemExit("%s: %s -> %s has no uuid" % (name, key, chart["file"]))
        if meta.get("sliceName") and chart["slice_name"] != meta["sliceName"]:
            raise SystemExit(
                "%s: %s says sliceName %r but chartId %s is %r (%s)"
                % (name, key, meta["sliceName"], cid, chart["slice_name"], chart["file"])
            )
        bound = meta.get("uuid")
        if bound and str(bound).lower() != chart["uuid"].lower():
            raise SystemExit(
                "%s: %s already carries uuid %s, which is not chartId %s's (%s)"
                % (name, key, bound, cid, chart["uuid"])
            )
        meta["uuid"] = chart["uuid"]
        node["meta"] = meta

    # 2. one slot per chart: keep the uuid-keyed node, drop the legacy duplicate.
    by_uuid = {}
    for key, node in nodes.items():
        by_uuid.setdefault(node["meta"]["uuid"].lower(), []).append(key)
    dropped = []
    for uuid, keys in by_uuid.items():
        if len(keys) < 2:
            continue
        keyed = [k for k in keys if re.match(r"^CHART-[A-Z0-9]{8}$", k)]
        keep = sorted(keyed or keys)[0]
        for key in keys:
            if key != keep:
                dropped.append((key, nodes[key]["meta"].get("sliceName")))
                del position[key]
        reports.append(
            {"action": "dropped-duplicate", "detail": "kept %s, dropped %s (%s)"
             % (keep, ", ".join(k for k in keys if k != keep), nodes[keep]["meta"].get("sliceName"))}
        )

    # 3. rows (and their grid entry) that lost all of their children.
    for key in list(position):
        node = position[key]
        if not isinstance(node, dict) or node.get("type") != "ROW":
            continue
        before_children = list(node.get("children") or [])
        node["children"] = [c for c in before_children if c in position]
        if before_children and not node["children"]:
            del position[key]
            grid = position.get("GRID_ID")
            if isinstance(grid, dict) and key in (grid.get("children") or []):
                grid["children"] = [c for c in grid["children"] if c != key]
            reports.append({"action": "dropped-empty-row", "detail": key})

    new_text = splice_position(text, position)
    back = yaml.safe_load(new_text)
    if back["position"] != position:
        raise SystemExit("%s: position did not round-trip — refusing to write" % name)
    for key in ("uuid", "dashboard_title", "metadata"):
        if (back.get(key) or {}) != (doc.get(key) or {}):
            raise SystemExit("%s: %s changed — refusing to write" % (name, key))

    after = sum(1 for v in position.values() if isinstance(v, dict) and v.get("type") == "CHART")
    if after != len(by_uuid):
        raise SystemExit(
            "%s: %d CHART nodes left but %d distinct charts" % (name, after, len(by_uuid))
        )

    unbound = [
        k for k, v in position.items()
        if isinstance(v, dict) and v.get("type") == "CHART" and not v["meta"].get("uuid")
    ]
    if unbound:
        raise SystemExit("%s: %s still unbound" % (name, ", ".join(unbound)))

    # Idempotence: nothing to bind, nothing duplicated, no row emptied -> the file
    # is already the bound form, so leave its bytes alone.
    if position == original:
        return None, {"action": "ok", "detail": "%d tiles already bound" % after}

    reports.insert(
        0,
        {
            "action": "bound",
            "detail": "%d CHART nodes -> %d tiles, each carrying its chart uuid"
            % (before, after),
        },
    )
    return new_text, {"action": "needs-repair", "detail": "; ".join(
        "%s: %s" % (r["action"], r["detail"]) for r in reports
    )}


def _effective(entries, filename, replacements):
    """An archive entry's bytes as they will be after this run."""
    for info, data in entries:
        if info.filename == filename:
            return replacements.get(filename, data)
    raise SystemExit("no such entry: %s" % filename)


def named_chart_uuids(text):
    """The (lowercased) chart uuids a dashboard's layout names."""
    pos = (yaml.safe_load(text) or {}).get("position") or {}
    out = set()
    for node in pos.values():
        if isinstance(node, dict) and node.get("type") == "CHART":
            uuid = str((node.get("meta") or {}).get("uuid") or "").lower()
            if uuid:
                out.add(uuid)
    return out


def _objects(data):
    """The document(s) in one datasets/ or databases/ entry (one per file)."""
    try:
        doc = yaml.safe_load(data.decode("utf-8")) or {}
    except yaml.YAMLError as exc:
        raise SystemExit("a datasets/databases entry is not a single YAML document "
                         "(%s) — refusing to prune" % exc)
    if isinstance(doc, list):
        raise SystemExit("a datasets/databases entry holds %d objects; the export "
                         "writes one per file — refusing to prune" % len(doc))
    return [doc]


def closure_orphans(entries, survivors, replacements):
    """
    [(filename, action, detail)] for everything the surviving dashboards do not
    need. Superset's dashboard import imports a chart only when a shipped layout
    names it, a dataset only when a shipped chart (or a native filter) points at
    it, and a database only when a shipped dataset points at it — so anything
    outside that closure is never imported, and install.sh's check_zip_landed()
    reports it MISSING and fails the run.
    """
    needed_charts = set()
    for text in survivors.values():
        needed_charts |= named_chart_uuids(text)

    chart_datasets = {}
    for info, data in entries:
        fn = info.filename
        if "/charts/" not in fn or not fn.endswith(".yaml"):
            continue
        doc = yaml.safe_load(_effective(entries, fn, replacements).decode("utf-8")) or {}
        chart_datasets[str(doc.get("uuid") or "").lower()] = str(
            doc.get("dataset_uuid") or ""
        ).lower()
    needed_datasets = {chart_datasets[u] for u in needed_charts if u in chart_datasets}

    reports = []
    for info, data in entries:
        fn = info.filename
        if "/charts/" in fn and fn.endswith(".yaml"):
            doc = yaml.safe_load(_effective(entries, fn, replacements).decode("utf-8")) or {}
            if str(doc.get("uuid") or "").lower() not in needed_charts:
                reports.append(
                    (fn, "orphan-chart",
                     "%s — no shipped dashboard names it, so the import skips it"
                     % (doc.get("slice_name") or os.path.basename(fn)))
                )
    dataset_docs = {}
    for info, data in entries:
        fn = info.filename
        if "/datasets/" not in fn or not fn.endswith(".yaml"):
            continue
        doc = _objects(data)[0]
        uuid = str(doc.get("uuid") or "").lower()
        dataset_docs[fn] = doc
        if uuid not in needed_datasets:
            reports.append(
                (fn, "orphan-dataset",
                 "%s.%s — no shipped chart points at it"
                 % (doc.get("schema"), doc.get("table_name")))
            )
    needed_databases = {
        str(doc.get("database_uuid") or "").lower()
        for fn, doc in dataset_docs.items()
        if fn not in {r[0] for r in reports if r[1] == "orphan-dataset"}
    }
    for info, data in entries:
        fn = info.filename
        if "/databases/" not in fn or not fn.endswith(".yaml"):
            continue
        doc = _objects(data)[0]
        if str(doc.get("uuid") or "").lower() not in needed_databases:
            reports.append(
                (fn, "orphan-database",
                 "%s — no shipped dataset points at it" % (doc.get("database_name") or fn))
            )
    return reports


def main():
    ap = argparse.ArgumentParser(
        description="Bind the bundled dashboards' layout to their chart uuids."
    )
    ap.add_argument("zip", nargs="?", default=DEFAULT_ZIP, help="bundle to repair (default: %(default)s)")
    ap.add_argument("--check", action="store_true", help="report only; exit 1 if the bundle is not bound")
    ap.add_argument("--retired", default=RETIRED_FILE,
                    help="retired-dashboard list (default: %(default)s)")
    args = ap.parse_args()

    if not os.path.isfile(args.zip):
        raise SystemExit("no such bundle: %s" % args.zip)

    entries = read_entries(args.zip)
    charts = chart_index(entries)
    retired = read_retired(args.retired)
    dash_files = [
        (i, d) for i, d in entries
        if "/dashboards/" in i.filename and i.filename.endswith(".yaml")
    ]

    replacements = {}
    dropped = set()
    reports = []
    survivors = {}          # dashboards/<file>.yaml -> its effective text
    for info, data in dash_files:
        name = os.path.basename(info.filename)
        text = data.decode("utf-8")
        doc = yaml.safe_load(text) or {}
        uuid = str(doc.get("uuid") or "").lower()
        if uuid and uuid in retired:
            dropped.add(info.filename)
            reports.append(
                {
                    "file": name,
                    "action": "retired",
                    "detail": "%s (%s) — dropped from the bundle"
                    % (doc.get("dashboard_title"), uuid),
                }
            )
            continue
        new_text, report = rebind_dashboard(text, charts, name)
        report["file"] = name
        reports.append(report)
        if new_text is not None:
            replacements[info.filename] = new_text.encode("utf-8")
            text = new_text
        survivors[info.filename] = text

    # Step 4: ship exactly the closure of the dashboards that survive.
    for fn, action, detail in closure_orphans(entries, survivors, replacements):
        dropped.add(fn)
        reports.append({"file": os.path.basename(fn), "action": action, "detail": detail})

    reports.sort(key=lambda r: (r["file"], r["action"]))
    for r in reports:
        print("  %-18s %-56s %s" % (r["action"], r["file"], r["detail"]))

    need = sum(1 for r in reports if r["action"] != "ok")
    retired_n = sum(1 for r in reports if r["action"] == "retired")
    print(
        "\n%s: %d dashboard(s) shipped, %d chart(s) — %d need repair, %d already bound"
        % (os.path.basename(args.zip), len(dash_files), len(charts), need, len(reports) - need)
    )

    if args.check:
        if need:
            print("FAIL: %d dashboard(s) are not bound" % need)
            return 1
        print("OK: every dashboard's tiles carry their chart's uuid")
        return 0

    if not replacements and not dropped:
        print("nothing to do — bundle already bound")
        return 0

    write_entries(args.zip, entries, replacements, dropped)

    # Prove it landed by reading the archive back.
    after = read_entries(args.zip)
    after_names = {i.filename for i, _ in after}
    for gone in dropped:
        if gone in after_names:
            raise SystemExit("%s: still in the archive after the rewrite" % gone)
    after_charts = chart_index(after)
    after_survivors = {}
    for info, data in after:
        if "/dashboards/" not in info.filename or not info.filename.endswith(".yaml"):
            continue
        position = yaml.safe_load(data.decode("utf-8"))["position"]
        bound = [
            v for v in position.values()
            if isinstance(v, dict) and v.get("type") == "CHART"
        ]
        uuids = [v["meta"].get("uuid", "").lower() for v in bound]
        if not bound:
            raise SystemExit("%s: no tiles left after the rewrite" % info.filename)
        if any(not u for u in uuids):
            raise SystemExit("%s: a tile is still unbound after the rewrite" % info.filename)
        if len(set(uuids)) != len(uuids):
            raise SystemExit("%s: a chart is still on the dashboard twice" % info.filename)
        unknown = [u for u in uuids if u not in {c["uuid"].lower() for c in after_charts.values()}]
        if unknown:
            raise SystemExit("%s: tiles name charts the bundle does not ship: %s" % (info.filename, unknown))
        after_survivors[info.filename] = data.decode("utf-8")
    # ...and that nothing outside the closure is left behind (the reverse
    # assertion: a chart/dataset/database no dashboard needs would be skipped by
    # the import and reported MISSING by install.sh's check_zip_landed).
    leftovers = closure_orphans(after, after_survivors, {})
    if leftovers:
        raise SystemExit(
            "still shipping %d object(s) no dashboard needs: %s"
            % (len(leftovers), ", ".join(fn for fn, _a, _d in leftovers))
        )
    print(
        "wrote %s — %d dashboard(s) left, %d entry(ies) dropped (%d retired), "
        "%d chart(s) shipped"
        % (args.zip, len(survivors), len(dropped), retired_n, len(after_charts))
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
