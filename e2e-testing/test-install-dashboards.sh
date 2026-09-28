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
#      `--dashboards-only`;
#   4. assert the REAL bundle in the repo — not just the synthetic fixture —
#      carries no pie / big_number chart with only the plural `metrics`; that
#      shape renders "Unexpected error" forever, invisibly to every API and DB
#      gate, and a re-exported bundle must not be able to re-introduce it.
#
# The bundle is synthetic but built with the real bundle's shape (dashboards/,
# charts/, datasets/<db>/, databases/, metadata.yaml, `uuid:`, `table_name:`,
# `schema:`, `slice_name:`, `dashboard_title:` — the real counts are not asserted
# here), and in the directory/field layout the Superset importer and the report
# both need.
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
# The stub's Superset state: which dashboards show a stale/duplicated link set.
# Reset before a case that must start from a broken instance; left alone to prove
# that a second --dashboards-only is a no-op.
STATE="$WORK/link-state"
export STUB_LINK_STATE="$STATE"
# The chart rows a previous seed generation left behind, which install.sh prunes
# after the reconcile: the stub reports them once, then not again.
export STUB_PRUNE_STATE="$WORK/prune-state"
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

# verify_superset() also runs superset/verify_dataset_metadata.sh, a live-EDW
# check this off-host fixture cannot satisfy. Stub it so the gate's dataset
# branch is exercised by the script's presence and exit code — without it the
# case reports the missing-script warning, which is not what is under test here.
mkdir -p "$WORK/tree/superset"
printf '#!/usr/bin/env bash\necho "  (stub) dataset column metadata check"\nexit 0\n' \
  > "$WORK/tree/superset/verify_dataset_metadata.sh"
chmod +x "$WORK/tree/superset/verify_dataset_metadata.sh"

run_case() { # name expected_rc install_dir [extra VAR=VAL ...]
  local name="$1" want="$2" dir="$3"
  shift 3
  local scenario="$name"
  case "$name" in
    *-lenient)   scenario="${name%-lenient}" ;;
    clean-again) scenario="clean" ;;        # same instance, second run
  esac
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
rm -f "$STATE"
run_case clean 0 "$WORK/tree"
contains clean "$CASE_OUT" "marts ready:"
contains clean "$CASE_OUT" "imported verisim_grocery_dashboards.zip (HTTP 200, overwrite=true)"
contains clean "$CASE_OUT" "3/3 dashboards present in Superset"
contains clean "$CASE_OUT" "18/18 charts present in Superset"
contains clean "$CASE_OUT" "8/8 datasets present in Superset"
contains clean "$CASE_OUT" "every chart has a query_context"
contains clean "$CASE_OUT" "dashboards: 14 (>= 11)"
contains clean "$CASE_OUT" "per dashboard:"
contains clean "$CASE_OUT" "dashboards-only complete."

echo
echo "== the import leaves the link sets stale and the gate reconciles them =="
contains clean "$CASE_OUT" 'dashboard 20 "Verisim Overview 1": 8 tile link(s) for 8 layout slot(s) (was 16)'
contains clean "$CASE_OUT" 'dashboard 21 "Verisim Overview 2": 6 tile link(s) for 6 layout slot(s)'
contains clean "$CASE_OUT" 'dashboard 22 "Verisim Overview 3": 4 tile link(s) for 4 layout slot(s) (was 3)'
contains clean "$CASE_OUT" "reconciled: 8 orphan link(s) unlinked, 1 missing link(s) linked"

echo
echo "== ...and the chart rows the import displaced are deleted, not left dead =="
# A chart converges by uuid only, so the generation a seed built before the
# import is a second row per chart name next to the one the layout places. The
# prune (prune_superseded_charts) deletes the ones no layout names and no
# dashboard links; the fixture's rows 3 and 4 are the two it reports.
contains clean "$CASE_OUT" "pruned: 2 superseded chart(s)"
contains clean "$CASE_OUT" "pruned superseded chart 'Chart 01 Daily Revenue' (id=3)"
contains clean "$CASE_OUT" "pruned superseded chart 'Chart 02 Location Performance' (id=4)"

echo
echo "== ...and a second run is a no-op (the links already match the layouts) =="
run_case clean-again 0 "$WORK/tree"
contains clean-again "$CASE_OUT" "reconciled: 0 orphan link(s) unlinked, 0 missing link(s) linked"
contains clean-again "$CASE_OUT" 'dashboard 20 "Verisim Overview 1": 8 tile link(s) for 8 layout slot(s)'
absent clean-again "$CASE_OUT" "(was 16)"
contains clean-again "$CASE_OUT" "pruned: 0 superseded chart(s)"
absent clean-again "$CASE_OUT" "pruned superseded chart"

echo
echo "== the prune's own DELETE calls go to the chart API, one per row =="
for needle in '"DELETE"' '"http://localhost:8088/api/v1/chart/3"' '"http://localhost:8088/api/v1/chart/4"' 'Bearer STUB.TOKEN'; do
  if grep -qF -- "$needle" "$STUB_IMPORT_LOG"; then ok "prune call carries $needle"; else bad "prune call missing $needle"; fi
done

echo
echo "== a delete the API refuses fails the run (a chart it still references) =="
rm -f "$STATE" "$STUB_PRUNE_STATE"
run_case prune422 1 "$WORK/tree"
contains prune422 "$CASE_OUT" "prune superseded chart 'Chart 01 Daily Revenue' (id=3) -> HTTP 422"
contains prune422 "$CASE_OUT" "Superset dashboards incomplete"
absent prune422 "$CASE_OUT" "pruned: 2 superseded chart(s)"

echo
echo "== a layout slot with no chart behind it fails the run (a tile that cannot render) =="
rm -f "$STATE"
run_case dangling 1 "$WORK/tree"
contains dangling "$CASE_OUT" "layout slot(s) name a chart that does not exist"
contains dangling "$CASE_OUT" "those tiles cannot render"
contains dangling "$CASE_OUT" "Superset dashboards incomplete"

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
rm -f "$STATE"   # a broken instance is on hand, so "no reconcile line" means the gate
run_case import422 1 "$WORK/tree"
contains import422 "$CASE_OUT" "HTTP 422"
contains import422 "$CASE_OUT" "was not passed"
contains import422 "$CASE_OUT" "nothing was imported: the request is atomic"
absent import422 "$CASE_OUT" "imported verisim_grocery_dashboards.zip"
absent import422 "$CASE_OUT" "reconciled:"   # nothing landed, so no layout was reconciled

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

echo
echo "== install path still backgrounds the import behind the mart gate =="
grep -qF 'superset_dashboards' "$SRC" && ok "main() calls superset_dashboards" || bad "main() does not gate on superset_dashboards"
grep -qF 'wait_for_marts' "$SRC" && ok "the mart wait is present" || bad "no mart wait"

echo
echo "== the bundle the repo SHIPS: no pie/big_number chart without the singular metric =="
# The fixture above is synthetic; this reads the real bundle install.sh would
# import, through the stdlib-only gate in lib/bundle_metrics.py (a guest running
# this suite has no PyYAML). See that file for why this shape is fatal: the tile
# renders "Unexpected error" forever while every API and DB gate stays green.
BUNDLE_DIR="$(cd "$H/.." && pwd)/superset/dashboards"
BUNDLES=("$BUNDLE_DIR"/*.zip)
if [ ! -e "${BUNDLES[0]}" ]; then
  bad "no bundled dashboard zip found in $BUNDLE_DIR"
else
  for bundle in "${BUNDLES[@]}"; do
    label="bundle $(basename "$bundle")"
    OUT_BUNDLE="$(python3 "$H/lib/bundle_metrics.py" "$bundle" 2>&1)"; BUNDLE_RC=$?
    printf '%s\n' "$OUT_BUNDLE" | sed 's/^/    /'
    rc_is "$label: singular metric on every pie/big_number chart" "$BUNDLE_RC" 0
  done
fi

echo
echo "== ...and that gate fails on the shape it exists for (control) =="
# A gate that only ever sees a clean bundle proves nothing: build the shape the
# defect had (a pie whose params carry the plural metrics and no metric) and
# require the gate to refuse it. Both controls are synthetic, 1 chart each.
cat > "$WORK/mkchart.py" <<'PY'
import sys, zipfile

FLAVOURS = {
    "plural": "  metrics:\n  - expressionType: SIMPLE\n    aggregate: SUM\n    label: SUM(quantity_on_hand)\n",
    "singular": "  metric:\n    expressionType: SIMPLE\n    aggregate: SUM\n    label: SUM(quantity_on_hand)\n",
    "null": "  metric: null\n  metrics:\n  - expressionType: SIMPLE\n    aggregate: SUM\n    label: SUM(quantity_on_hand)\n",
}
body = ("""slice_name: Stock Aging Breakdown
viz_type: pie
params:
  datasource: 15__table
  viz_type: pie
  time_range: No filter
  adhoc_filters: []
%s  groupby:
  - stock_aging_category
  row_limit: 10
query_context: '{"datasource": {"id": 15, "type": "table"}, "queries": []}'
uuid: a25b3b1f-052a-420e-a7a4-06eeed88b5c3
dataset_uuid: 19e463d2-35f2-460a-8ac1-b679678d2463
""" % FLAVOURS[sys.argv[2]])
with zipfile.ZipFile(sys.argv[1], "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("chart_export/charts/Stock_Aging_Breakdown_37.yaml", body)
PY
for flavour in plural null; do
  python3 "$WORK/mkchart.py" "$WORK/gate-$flavour.zip" "$flavour"
  OUT_GATE="$(python3 "$H/lib/bundle_metrics.py" "$WORK/gate-$flavour.zip" 2>&1)"; GATE_RC=$?
  printf '%s\n' "$OUT_GATE" | sed 's/^/    /'
  rc_is "gate refuses the $flavour shape" "$GATE_RC" 1
  contains "gate names the $flavour offence" "$OUT_GATE" "VIOLATION"
done
python3 "$WORK/mkchart.py" "$WORK/gate-singular.zip" singular
OUT_GATE="$(python3 "$H/lib/bundle_metrics.py" "$WORK/gate-singular.zip" 2>&1)"; GATE_RC=$?
rc_is "gate accepts the singular shape" "$GATE_RC" 0
absent "gate reports no offence for the singular shape" "$OUT_GATE" "VIOLATION"

echo
echo "== wiring: one rule, shared by the seed, the bundle repair and this gate =="
ROOT="$(cd "$H/.." && pwd)"
grep -qF 'prune_superseded_charts "$dash_pairs"' "$SRC" \
  && ok "the install step prunes the chart rows an import displaced" \
  || bad "install.sh never prunes a superseded chart generation"
grep -qF '_superset_chart_params.py' "$ROOT/init.sh" \
  && ok "init.sh copies the shared rule into _conf" || bad "init.sh does not copy the shared rule"
grep -qF 'normalise_metric_params' "$ROOT/superset/create_grocery_ops_dashboard.py" \
  && ok "the grocery-ops seed applies the rule" || bad "the seed normalises its charts inline"
grep -qF 'normalise_metric_params' "$ROOT/superset/dashboards/normalise_chart_metrics.py" \
  && ok "the offline bundle repair applies the rule" || bad "the bundle repair does not apply the rule"

echo
echo "test-install-dashboards.sh: $PASS passed, $FAIL failed"
[ "$FAIL" = "0" ] || exit 1
