#!/usr/bin/env bash
# Offline tests for superset/dashboards/bind_dashboard_layout.py — the tool that
# rewrites the bundled dashboard export.
#
# Two things it owns, both asserted here against the REAL tool and a synthetic
# bundle (no Superset, no docker):
#
#   1. BIND — every CHART node carries the uuid of the chart it names, a chart
#      twice on one dashboard is dropped, a node naming a chart the bundle does
#      not ship is an error (a dangling tile would render "There is no chart
#      definition associated with this component" on the target instance);
#   2. PRUNE to the closure of the dashboards that survive — a retired dashboard
#      is dropped, and so are the charts no shipped layout names, the datasets
#      no shipped chart points at, and the databases no shipped dataset points
#      at. Superset's dashboard import only imports a chart a shipped layout
#      names, so an orphan in the archive is never imported — and install.sh's
#      check_zip_landed() asserts every bundled object LANDED, so it fails
#      `install.sh --dashboards-only` with "only 10/18 charts present"
#      (t_23a97d10: retiring a dashboard without its charts turned the install
#      step red).
#
#   bash e2e-testing/test-bind-dashboard-layout.sh
set -uo pipefail
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOL="$(cd "$H/.." && pwd)/superset/dashboards/bind_dashboard_layout.py"
WORK="$H/logs/bind-dashboard-layout"
rm -rf "$WORK"
mkdir -p "$WORK"

[ -f "$TOOL" ] || { echo "no tool at $TOOL"; exit 1; }

# The tool is a WORKSTATION tool: it imports PyYAML to rewrite the export, and
# the slot's bare Debian host has none (that is why the offline stub for the
# install path parses the bundle with re, not yaml). Say so instead of reporting
# a red that cannot be fixed where it is run.
if ! python3 -c 'import yaml' 2>/dev/null; then
  echo "SKIP: no PyYAML on this host — bind_dashboard_layout.py is a workstation"
  echo "      tool (it runs where the export is edited, not on the slot)."
  echo "      No assertion ran; run this test from the workstation. exit 0"
  exit 0
fi

PASS=0; FAIL=0
ok()  { echo "  PASS  $*"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL  $*"; FAIL=$((FAIL + 1)); }
rc_is() { # label got want
  if [ "$2" = "$3" ]; then ok "$1: exit $2"; else bad "$1: exit $2 (want $3)"; fi
}
contains() { # label haystack needle
  if printf '%s' "$2" | grep -qF -- "$3"; then ok "$1: $3"; else bad "$1: missing '$3'"; fi
}
in_zip() { # label zip entry
  if unzip -l "$2" | grep -qF -- "$3"; then ok "$1: $3 present"; else bad "$1: $3 missing"; fi
}
not_in_zip() { # label zip entry
  if unzip -l "$2" | grep -qF -- "$3"; then bad "$1: $3 still shipped"; else ok "$1: $3 dropped"; fi
}

cat > "$WORK/mkbundle.py" <<'PY'
"""A synthetic bundle carrying every case the pruner has to handle."""
import sys
import zipfile

import yaml

ROOT = "dashboard_export_test"


def db_yaml(name, uuid):
    return ("database_name: %s\nsqlalchemy_uri: postgresql://u:p@h:5432/db\n"
            "uuid: %s\nversion: 1.0.0\n" % (name, uuid))


def ds_yaml(table, uuid, db_uuid):
    return ("table_name: %s\nschema: mart\nuuid: %s\ndatabase_uuid: %s\n"
            "columns:\n- column_name: revenue\n  is_dttm: false\n  type: NUMERIC\n"
            % (table, uuid, db_uuid))


def chart_yaml(name, uuid, ds_uuid):
    return ("slice_name: %s\nviz_type: big_number_total\nparams:\n  viz_type: big_number_total\n"
            "query_context: null\ncache_timeout: null\nuuid: %s\nversion: 1.0.0\n"
            "dataset_uuid: %s\n" % (name, uuid, ds_uuid))


def position(nodes):
    """nodes: [(key, chartId, sliceName, uuid_or_None)] -> a position mapping."""
    out = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
           "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-0"],
                       "parents": ["ROOT_ID"]}}
    kids = []
    for key, cid, sname, uuid in nodes:
        kids.append(key)
        meta = {"chartId": cid, "width": 6, "height": 60, "sliceName": sname}
        if uuid:
            meta["uuid"] = uuid
        out[key] = {"type": "CHART", "id": key, "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", "ROW-0"], "meta": meta}
    out["ROW-0"] = {"type": "ROW", "id": "ROW-0", "children": kids,
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    return out


def dash_yaml(title, uuid, nodes):
    return yaml.safe_dump(
        {"dashboard_title": title, "description": None, "slug": None, "published": True,
         "uuid": uuid, "position": position(nodes), "metadata": {}, "version": "1.0.0"},
        default_flow_style=False, sort_keys=False, width=10 ** 9)


def write(path):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("%s/metadata.yaml" % ROOT,
                   "version: 1.0.0\ntype: Dashboard\ntimestamp: '2026-09-21T00:00:00+00:00'\n")
        z.writestr("%s/databases/Grocery.yaml" % ROOT, db_yaml("Grocery", "db1"))
        z.writestr("%s/databases/Other.yaml" % ROOT, db_yaml("Other", "db2"))
        z.writestr("%s/datasets/Grocery/shared_table.yaml" % ROOT,
                   ds_yaml("shared_table", "ds1", "db1"))
        z.writestr("%s/datasets/Grocery/orphan_table.yaml" % ROOT,
                   ds_yaml("orphan_table", "ds2", "db1"))
        z.writestr("%s/datasets/Other/other_table.yaml" % ROOT,
                   ds_yaml("other_table", "ds3", "db2"))
        # charts: two shared by the kept dashboard, one nobody names, one that
        # only the retired dashboard names (and it is the only user of db2).
        z.writestr("%s/charts/Shared_A_1.yaml" % ROOT, chart_yaml("Shared A", "ch1", "ds1"))
        z.writestr("%s/charts/Shared_B_2.yaml" % ROOT, chart_yaml("Shared B", "ch2", "ds1"))
        z.writestr("%s/charts/Orphan_3.yaml" % ROOT, chart_yaml("Orphan", "ch3", "ds2"))
        z.writestr("%s/charts/Retired_4.yaml" % ROOT, chart_yaml("Retired Chart", "ch4", "ds3"))
        z.writestr("%s/dashboards/Keep_1.yaml" % ROOT,
                   dash_yaml("Keep Me", "keep-uuid",
                             [("CHART-AAAAAAAA", 1, "Shared A", "ch1"),
                              ("CHART-BBBBBBBB", 2, "Shared B", "ch2")]))
        z.writestr("%s/dashboards/Retired_2.yaml" % ROOT,
                   dash_yaml("Retired Two", "retire-uuid",
                             [("CHART-CCCCCCCC", 4, "Retired Chart", "ch4")]))
    print("  built %s" % path)


if __name__ == "__main__":
    write(sys.argv[1])
PY

cat > "$WORK/retired.txt" <<'TXT'
# synthetic retired list (the real one carries Gas Station + Grocery Overview)
retire-uuid	Retired Two
TXT

# rewrite.py <src> <dst> <mode> — fixture mutations, one file each below.
cat > "$WORK/rewrite.py" <<'PY'
"""Copy a bundle, mutating what MODE says. Stdlib + PyYAML only."""
import sys
import zipfile

import yaml

src, dst, mode = sys.argv[1], sys.argv[2], sys.argv[3]


def mutate(filename, text):
    doc = yaml.safe_load(text)
    if mode == "dangle":
        if filename.endswith("dashboards/Keep_1.yaml"):
            doc["position"]["CHART-AAAAAAAA"]["meta"]["chartId"] = 99
    elif mode == "unbind":
        if filename.endswith("dashboards/Keep_1.yaml"):
            for node in doc["position"].values():
                if isinstance(node, dict) and node.get("type") == "CHART":
                    node["meta"].pop("uuid", None)
    return yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, width=10 ** 9)


with zipfile.ZipFile(src) as zin:
    entries = [(i, zin.read(i.filename)) for i in zin.infolist()]
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
    for info, data in entries:
        if mode == "multi" and info.filename.endswith("datasets/Grocery/shared_table.yaml"):
            data = (data.decode() + "---\n" + data.decode()).encode()
        elif mode in ("dangle", "unbind") and info.filename.endswith(".yaml") and "/dashboards/" in info.filename:
            data = mutate(info.filename, data.decode()).encode()
        z.writestr(info.filename, data)
print("  built %s (%s)" % (dst, mode))
PY

echo "== fixture =="
python3 "$WORK/mkbundle.py" "$WORK/dirty.zip" >/dev/null && ok "synthetic bundle built"
cp "$WORK/dirty.zip" "$WORK/clean.zip"

run_tool() { # zip [extra args...] -> OUT, RC
  OUT="$(python3 "$TOOL" --retired "$WORK/retired.txt" "$@" 2>&1)"; RC=$?
}

echo
echo "== 1. a bundle that ships a retired dashboard and orphans fails --check, and writes nothing =="
run_tool --check "$WORK/dirty.zip"
rc_is "dirty --check" "$RC" 1
contains "dirty --check" "$OUT" "FAIL"
contains "dirty --check names the retired dashboard" "$OUT" "retired            Retired_2.yaml"
contains "dirty --check names the orphan chart" "$OUT" "orphan-chart"
contains "dirty --check names the orphan dataset" "$OUT" "orphan-dataset"
contains "dirty --check names the orphan database" "$OUT" "orphan-database"
in_zip "dirty --check is read-only" "$WORK/dirty.zip" "charts/Orphan_3.yaml"
in_zip "dirty --check is read-only (retired dashboard)" "$WORK/dirty.zip" "dashboards/Retired_2.yaml"

echo
echo "== 2. the run prunes to the closure (and proves it by reading the zip back) =="
run_tool "$WORK/clean.zip"
rc_is "prune run" "$RC" 0
contains "prune run" "$OUT" "1 dashboard(s) left"
contains "prune run counted the drops" "$OUT" "6 entry(ies) dropped"
contains "prune run counted the retire" "$OUT" "(1 retired)"
in_zip "survivor" "$WORK/clean.zip" "dashboards/Keep_1.yaml"
in_zip "survivor" "$WORK/clean.zip" "charts/Shared_A_1.yaml"
in_zip "survivor" "$WORK/clean.zip" "charts/Shared_B_2.yaml"
in_zip "survivor" "$WORK/clean.zip" "datasets/Grocery/shared_table.yaml"
in_zip "survivor" "$WORK/clean.zip" "databases/Grocery.yaml"
not_in_zip "retired dashboard" "$WORK/clean.zip" "dashboards/Retired_2.yaml"
not_in_zip "orphan chart" "$WORK/clean.zip" "charts/Orphan_3.yaml"
not_in_zip "retired dashboard's chart" "$WORK/clean.zip" "charts/Retired_4.yaml"
not_in_zip "orphan dataset" "$WORK/clean.zip" "datasets/Grocery/orphan_table.yaml"
not_in_zip "retired chart's dataset" "$WORK/clean.zip" "datasets/Other/other_table.yaml"
not_in_zip "orphan database" "$WORK/clean.zip" "databases/Other.yaml"

echo
echo "== 3. idempotent: a second run writes nothing, --check is clean =="
run_tool "$WORK/clean.zip"
rc_is "second run" "$RC" 0
contains "second run" "$OUT" "nothing to do — bundle already bound"
run_tool --check "$WORK/clean.zip"
rc_is "clean --check" "$RC" 0
contains "clean --check" "$OUT" "OK: every dashboard's tiles carry their chart's uuid"

echo
echo "== 4. a tile naming a chart the bundle does not ship is an error =="
python3 "$WORK/rewrite.py" "$WORK/dirty.zip" "$WORK/dangling.zip" dangle >/dev/null
run_tool "$WORK/dangling.zip"
if [ "$RC" -ne 0 ]; then ok "dangling bundle: exit $RC"; else bad "dangling bundle: exit 0"; fi
contains "dangling bundle" "$OUT" "which the bundle does not ship"

echo
echo "== 5. a node without a uuid is bound (the tool's original job) =="
python3 "$WORK/rewrite.py" "$WORK/dirty.zip" "$WORK/unbound.zip" unbind >/dev/null
run_tool --check "$WORK/unbound.zip"
rc_is "unbound --check" "$RC" 1
run_tool "$WORK/unbound.zip"
rc_is "unbound repair" "$RC" 0
contains "unbound repair binds the node" "$OUT" "CHART nodes -> 2 tiles"
run_tool --check "$WORK/unbound.zip"
rc_is "unbound --check after repair" "$RC" 0

echo
echo "== 6. a multi-document datasets file is refused, not silently pruned =="
python3 "$WORK/rewrite.py" "$WORK/dirty.zip" "$WORK/multi.zip" multi >/dev/null
run_tool "$WORK/multi.zip"
if [ "$RC" -ne 0 ]; then ok "multi-document dataset: exit $RC"; else bad "multi-document dataset: exit 0"; fi
contains "multi-document dataset" "$OUT" "refusing to prune"

echo
echo "test-bind-dashboard-layout.sh: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
