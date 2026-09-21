#!/usr/bin/env bash
# Offline tests for install.sh's bundled-dashboard import (the block above main).
#
# That step has to do three things, and all three are asserted here against the
# REAL script with stub docker/curl:
#
#   1. not import before the marts exist — the bundle carries one Superset
#      dataset per mart table, so the charts would land with no table behind
#      them and Superset would still answer HTTP 200 {"message": "OK"}
#      (dev, 2026-09-21: 18 charts with query_context null, exactly this
#      bundle's charts — the gate in full-cycle.sh phase 8 fails on those);
#   2. import idempotently and capture the HTTP status AND the response body —
#      Superset 4.1.2 rejects a repeat import of the same bundle with 422
#      unless overwrite=true is passed, and a 422 looks exactly like a 200 to
#      the old `curl ... && log "Imported"` line;
#   3. report per object — every bundled dashboard/chart/dataset present or
#      MISSING, plus the gate's own two assertions (datasource_id,
#      query_context) — and let the verdict decide the exit status of
#      `--dashboards-only`.
#
# The bundle is synthetic but built with the real bundle's shape and counts
# (3 dashboards, 18 charts, 8 mart datasets, 1 database) and the same
# directory/field layout the Superset importer and the report both need:
# databases/, datasets/<db>/, charts/, dashboards/, metadata.yaml, `uuid:`,
# `table_name:`, `schema:`, `slice_name:`, `dashboard_title:`.
#
#   bash e2e-testing/test-install-dashboards.sh
#   INSTALL_SH=/path/to/install.sh bash e2e-testing/test-install-dashboards.sh
#
# Knobs are STUB_-prefixed because install.sh owns generically-named variables
# (INSTALL_DIR, DASH_DIR, DASH_WAIT, ...) and would overwrite a bare SCENARIO.
set -uo pipefail
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${INSTALL_SH:-$(cd "$H/.." && pwd)/install.sh}"
WORK="$H/logs/install-dashboards"
rm -rf "$WORK"
mkdir -p "$WORK/tree/superset/dashboards" "$WORK/empty/superset/dashboards"
ZIP="$WORK/tree/superset/dashboards/verisim_grocery_dashboards.zip"
export STUB_ZIP="$ZIP"
export STUB_IMPORT_LOG="$WORK/import-calls.jsonl"
export PATH="$H/bin-dashboards:$PATH"

[ -f "$SRC" ] || { echo "install.sh not found at $SRC"; exit 1; }

PASS=0; FAIL=0
ok()  { echo "  PASS  $*"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL  $*"; FAIL=$((FAIL + 1)); }
contains() { # label haystack needle
  if printf '%s' "$2" | grep -qF -- "$3"; then ok "$1: $3"; else bad "$1: missing '$3'"; fi
}
absent() { # label haystack needle
  if printf '%s' "$2" | grep -qF -- "$3"; then bad "$1: should not say '$3'"; else ok "$1: no '$3'"; fi
}
rc_is() { # label got want
  if [ "$2" = "$3" ]; then ok "$1: exit $2"; else bad "$1: exit $2 (want $3)"; fi
}

echo "== fixture: synthetic bundle (shape/counts of the real one) =="
cat > "$WORK/mkbundle.py" <<'PY'
import sys, zipfile

ROOT = "dashboard_export_test"
DASH_UUIDS = ["d0000000-0000-4000-8000-00000000000%d" % i for i in (1, 2, 3)]
TABLES = ["mart_daily_revenue", "mart_location_performance", "mart_department_performance",
          "mart_product_performance", "mart_inventory_turnover", "mart_hourly_sales_pattern",
          "mart_store_weekly_summary", "mart_delivery_performance"]
DATASETS = [("s%07d-0000-4000-8000-000000000001" % (i + 1), t) for i, t in enumerate(TABLES)]
DB_UUID = "b0000000-0000-4000-8000-000000000001"
CHART_NAMES = ["Chart %02d %s" % (i + 1, t.replace("mart_", "").replace("_", " ").title())
               for i in range(18) for t in [TABLES[i % len(TABLES)]]]
CHART_UUIDS = ["c0000000-0000-4000-8000-0000000000%02d" % i for i in range(1, len(CHART_NAMES) + 1)]


def dataset_yaml(table, uuid):
    return ("table_name: %s\nschema: mart\nuuid: %s\ndatabase_uuid: %s\n"
            "columns:\n- column_name: revenue\n  is_dttm: false\n  type: NUMERIC\n"
            % (table, uuid, DB_UUID))


def chart_yaml(name, uuid, dataset_uuid):
    return ("slice_name: %s\ndescription: null\nviz_type: big_number_total\n"
            "params:\n  viz_type: big_number_total\n  datasource: 1__table\n"
            "  metric:\n    expressionType: SIMPLE\n    column:\n      column_name: revenue\n"
            "      type: NUMERIC\n    aggregate: SUM\n    label: SUM(revenue)\n"
            "  adhoc_filters: []\n  time_range: No filter\n"
            "query_context: null\ncache_timeout: null\n"
            "uuid: %s\nversion: 1.0.0\ndataset_uuid: %s\n" % (name, uuid, dataset_uuid))


def dashboard_yaml(title, uuid, chart_uuids):
    body = ("dashboard_title: %s\ndescription: null\nslug: null\n"
            "published: false\nposition:\n  DASHBOARD_VERSION_KEY: v2\n"
            "uuid: %s\nversion: 1.0.0\n" % (title, uuid))
    return body + "".join("# chart %s\n" % u for u in chart_uuids)


with zipfile.ZipFile(sys.argv[1], "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("%s/metadata.yaml" % ROOT,
               "version: 1.0.0\ntype: Dashboard\ntimestamp: '2026-09-21T00:00:00+00:00'\n")
    z.writestr("%s/databases/Grocery.yaml" % ROOT,
               "database_name: Grocery\nsqlalchemy_uri: postgresql://postgres:***@postgres:5432/grocery\n"
               "uuid: %s\nversion: 1.0.0\n" % DB_UUID)
    for uuid, table in DATASETS:
        z.writestr("%s/datasets/Grocery/%s.yaml" % (ROOT, table), dataset_yaml(table, uuid))
    for i, (name, uuid) in enumerate(zip(CHART_NAMES, CHART_UUIDS)):
        _, table = DATASETS[i % len(DATASETS)]
        z.writestr("%s/charts/%s.yaml" % (ROOT, name.replace(" ", "_")),
                   chart_yaml(name, uuid, [d[0] for d in DATASETS][i % len(DATASETS)]))
    for i, duuid in enumerate(DASH_UUIDS):
        z.writestr("%s/dashboards/Verisim_Overview_%d.yaml" % (ROOT, i + 1),
                   dashboard_yaml("Verisim Overview %d" % (i + 1), duuid, CHART_UUIDS))
print("  built %s (3 dashboards, 18 charts, 8 datasets)" % sys.argv[1])
PY
python3 "$WORK/mkbundle.py" "$ZIP"
[ -f "$ZIP" ] && ok "fixture bundle exists" || bad "fixture bundle missing"
chmod +x "$H/bin-dashboards/docker" "$H/bin-dashboards/curl"

echo
echo "== the chart copy is derived with the stdlib only (a guest has no PyYAML) =="
# Pull the shipped snippet out of install.sh and run it under `python3 -S`, which
# disables site-packages — that is the bare Debian guest the deploy runs on, where
# `import yaml` fails. The chart refresh silently did nothing there until the
# derivation was rewritten against re/zipfile (found on dev 106, 2026-09-21).
sed -n "/^bundle_charts_zip()/,/^}/p" "$SRC" \
  | sed -n "/<<'PY'/,/^PY\$/p" | sed '1d;$d' > "$WORK/derive.py"
if [ -s "$WORK/derive.py" ]; then
  ok "extracted bundle_charts_zip() body ($(wc -l < "$WORK/derive.py") lines)"
else
  bad "could not extract bundle_charts_zip() from install.sh"
fi
rm -f "$WORK/charts-noyaml.zip"
if python3 -S - "$ZIP" "$WORK/charts-noyaml.zip" < "$WORK/derive.py" >/dev/null 2>"$WORK/derive.err"; then
  ok "the derivation runs under python3 -S (no site-packages)"
else
  bad "the derivation needs a module a guest does not have: $(tr '\n' ' ' < "$WORK/derive.err" | head -c 200)"
fi
DERIVED="$(python3 - "$WORK/charts-noyaml.zip" <<'PY' 2>/dev/null || true
import re
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as z:
    names = z.namelist()
    meta = [n for n in names if n.endswith("metadata.yaml")][0]
    print("type=%s dashboards=%d charts=%d datasets=%d"
          % (re.search(r"(?m)^type:\s*(\S+)", z.read(meta).decode()).group(1),
             sum(1 for n in names if "/dashboards/" in n),
             sum(1 for n in names if "/charts/" in n),
             sum(1 for n in names if "/datasets/" in n)))
PY
)"
contains "the derived chart copy" "$DERIVED" "type=Slice"
contains "the derived chart copy" "$DERIVED" "dashboards=0"
contains "the derived chart copy" "$DERIVED" "charts=18"

run_case() { # name expected_rc install_dir [extra VAR=VAL ...]
  local name="$1" want="$2" dir="$3"
  shift 3
  local scenario="$name"
  case "$name" in *-lenient) scenario="${name%-lenient}" ;; esac
  local out rc
  out="$(env STUB_SCENARIO="$scenario" STUB_ZIP="$ZIP" STUB_IMPORT_LOG="$STUB_IMPORT_LOG" \
        DASH_WAIT=0 INSTALL_DIR="$dir" "$@" bash "$SRC" --dashboards-only 2>&1)"
  rc=$?
  printf '%s\n' "$out" > "$WORK/out.$name"
  echo "--- $name (exit $rc) ---"
  printf '%s\n' "$out" | sed 's/^/    /'
  rc_is "$name" "$rc" "$want"
  CASE_OUT="$out"
}

echo
echo "== marts present, import complete: exit 0, every object accounted for =="
run_case clean 0 "$WORK/tree"
contains clean "$CASE_OUT" "marts ready:"
contains clean "$CASE_OUT" "imported verisim_grocery_dashboards.zip (HTTP 200, overwrite=true)"
contains clean "$CASE_OUT" "3/3 dashboards present in Superset"
contains clean "$CASE_OUT" "18/18 charts present in Superset"
contains clean "$CASE_OUT" "8/8 datasets present in Superset"
contains clean "$CASE_OUT" "every chart has a query_context"
contains clean "$CASE_OUT" "refreshed verisim_grocery_dashboards.zip charts"
contains clean "$CASE_OUT" "dashboards: 14 (>= 11)"
contains clean "$CASE_OUT" "per dashboard:"
contains clean "$CASE_OUT" "dashboards-only complete."

echo
echo "== marts present, the bundle's own charts landed without query_context: exit 1, named =="
run_case broken 1 "$WORK/tree"
contains broken "$CASE_OUT" "charts have no query_context"
contains broken "$CASE_OUT" "Chart has no query context saved"
contains broken "$CASE_OUT" "first 8:"
contains broken "$CASE_OUT" "Superset dashboards incomplete"

echo
echo "== marts present, only part of the bundle landed: exit 1, MISSING listed =="
run_case partial 1 "$WORK/tree"
contains partial "$CASE_OUT" "only 15/18 charts present"
contains partial "$CASE_OUT" "MISSING:"

echo
echo "== import rejected (HTTP 422): exit 1, Superset's per-object error printed =="
run_case import422 1 "$WORK/tree"
contains import422 "$CASE_OUT" "HTTP 422"
contains import422 "$CASE_OUT" "was not passed"
contains import422 "$CASE_OUT" "nothing was imported: the request is atomic"
absent import422 "$CASE_OUT" "imported verisim_grocery_dashboards.zip"

echo
echo "== the chart refresh is rejected (HTTP 422): exit 1, charts left as they were =="
run_case chart422 1 "$WORK/tree"
contains chart422 "$CASE_OUT" "imported verisim_grocery_dashboards.zip (HTTP 200, overwrite=true)"
contains chart422 "$CASE_OUT" "chart refresh for verisim_grocery_dashboards.zip -> HTTP 422"
contains chart422 "$CASE_OUT" "charts kept their old query_context"
contains chart422 "$CASE_OUT" "Chart already exists and \`overwrite=true\` was not passed"
contains chart422 "$CASE_OUT" "Superset dashboards incomplete"

echo
echo "== login refused: exit 1, nothing claimed as imported =="
run_case nologin 1 "$WORK/tree"
contains nologin "$CASE_OUT" "could not authenticate to Superset"
contains nologin "$CASE_OUT" "install.sh --dashboards-only"
absent nologin "$CASE_OUT" "imported verisim_grocery_dashboards.zip"

echo
echo "== transform mid-flight: exit 0, deferred, never claims an import =="
run_case deferred 0 "$WORK/tree"
contains deferred "$CASE_OUT" "deferred: importing now would ship charts"
contains deferred "$CASE_OUT" "install.sh --dashboards-only"
absent deferred "$CASE_OUT" "imported verisim_grocery_dashboards.zip"

echo
echo "== no mart schema at all (cold EDW): exit 0, deferred =="
run_case nomart 0 "$WORK/tree"
contains nomart "$CASE_OUT" "deferred: importing now would ship charts"

echo
echo "== 42 marts but one bundled dataset's table missing: exit 0, named =="
run_case missingmart 0 "$WORK/tree"
contains missingmart "$CASE_OUT" "marts still missing after 0s: mart_daily_revenue"
contains missingmart "$CASE_OUT" "deferred: importing now would ship charts"

echo
echo "== postgres down: exit 0, says so, claims nothing =="
run_case nopg 0 "$WORK/tree"
contains nopg "$CASE_OUT" "postgres is not running"
absent nopg "$CASE_OUT" "imported verisim_grocery_dashboards.zip"

echo
echo "== DASH_STRICT=0: the same incomplete import is a warning, exit 0 =="
run_case broken-lenient 0 "$WORK/tree" DASH_STRICT=0
contains broken-lenient "$CASE_OUT" "charts have no query_context"
contains broken-lenient "$CASE_OUT" "DASH_STRICT=0, so this run still exits 0"
contains broken-lenient "$CASE_OUT" "dashboards-only complete."

echo
echo "== no bundled zips: exit 0, nothing to do =="
run_case nozip 0 "$WORK/empty"
contains nozip "$CASE_OUT" "no bundled dashboards — nothing to import"

echo
echo "== the import call itself (formData + password map + overwrite + token) =="
for needle in 'formData=@' 'databases/Grocery.yaml' '"overwrite=true"' 'Bearer STUB.TOKEN'; do
  if grep -qF -- "$needle" "$STUB_IMPORT_LOG"; then ok "import call carries $needle"; else bad "import call missing $needle"; fi
done

echo "== the bundle's charts are refreshed through the CHART importer =="
if grep -qF '/api/v1/chart/import/' "$STUB_IMPORT_LOG"; then
  ok "chart refresh posted to /api/v1/chart/import/ (the importer that honours overwrite)"
else
  bad "no chart refresh call — an existing chart would keep whatever query_context it had"
fi

echo
echo "== install path still backgrounds the import behind the mart gate =="
grep -qF 'superset_dashboards' "$SRC" && ok "main() calls superset_dashboards" || bad "main() does not gate on superset_dashboards"
grep -qF 'wait_for_marts' "$SRC" && ok "the mart wait is present" || bad "no mart wait"

echo
echo "test-install-dashboards.sh: $PASS passed, $FAIL failed"
[ "$FAIL" = "0" ] || exit 1
