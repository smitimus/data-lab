#!/usr/bin/env bash
# Offline tests for superset/verify_dataset_metadata.sh — the stale-dataset gate.
#
# The gate's whole job is to see a defect class that every other gate misses: a
# Superset dataset whose registered columns no longer match the physical mart, which
# renders a browser tile broken while /api/v1/chart/<id>/data/, the meta-DB checks
# and full-cycle --verify all stay green. It has to fail on exactly that, and it has
# to fail LOUDLY rather than pass when it cannot read a database at all.
#
# The comparison reads two databases, so the cases are driven through the gate's
# EDW_LIST / SUP_LIST / SUP_DS_LIST hooks (documented in the gate itself) instead of
# a live slot: the logic under test is the comparison, not docker.
#
#   bash e2e-testing/test-dataset-metadata-gate.sh
#   GATE=/path/to/verify_dataset_metadata.sh bash e2e-testing/test-dataset-metadata-gate.sh
set -uo pipefail
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="${GATE:-$(cd "$H/.." && pwd)/superset/verify_dataset_metadata.sh}"
WORK="$H/logs/dataset-metadata-gate"
rm -rf "$WORK"; mkdir -p "$WORK"

[ -f "$GATE" ] || { echo "gate not found at $GATE"; exit 1; }

PASS=0; FAIL=0
ok()  { echo "  PASS  $*"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL  $*"; FAIL=$((FAIL + 1)); }
rc_is() { if [ "$2" = "$3" ]; then ok "$1: exit $2"; else bad "$1: exit $2 (want $3)"; fi; }
contains() { if printf '%s' "$2" | grep -qF -- "$3"; then ok "$1: $3"; else bad "$1: missing '$3'"; fi }

# ---------------------------------------------------------------- fixtures
# mart_a and mart_b are registered datasets; `spine` is an EDW mart with no dataset
# (the MetricFlow time spine in schema `mart` — informational, not a failure).
cat > "$WORK/edw.clean" <<'EOF'
mart_a|id
mart_a|d1
mart_a|as_of_date
mart_b|id
mart_b|v
spine|date_day
EOF
cat > "$WORK/sup.clean" <<'EOF'
mart_a|id
mart_a|d1
mart_a|as_of_date
mart_b|id
mart_b|v
EOF
cat > "$WORK/ds.clean" <<'EOF'
mart_a|1
mart_b|2
EOF

run() { # run <edw> <sup> <ds>  -> OUT, RC
  OUT="$(EDW_LIST="$WORK/$1" SUP_LIST="$WORK/$2" SUP_DS_LIST="$WORK/$3" bash "$GATE" 2>&1)"
  RC=$?
}

echo "== a clean instance: every dataset matches, the unregistered mart is a note =="
run edw.clean sup.clean ds.clean
rc_is clean "$RC" 0
contains clean "$OUT" "OK: all 2 dataset(s) in 'mart' match the EDW column-for-column"
contains clean "$OUT" "note: EDW mart without a Superset dataset: spine"
contains clean "$OUT" "EDW mart tables: 3   Superset mart datasets: 2"

echo
echo "== THE CARD's case: a mart gained as_of_date after the dataset was registered =="
grep -v '^mart_a|as_of_date$' "$WORK/sup.clean" > "$WORK/sup.missing"
run edw.clean sup.missing ds.clean
rc_is missing "$RC" 1
contains missing "$OUT" "✗ mart_a (dataset 1):"
contains missing "$OUT" "not in the dataset, the EDW has: as_of_date"
contains missing "$OUT" "STALE: 1 of 2 dataset(s)"
contains missing "$OUT" "_superset_dataset_metadata.py --refresh-all"

echo
echo "== the reverse: a column the EDW dropped/renamed still sits in the dataset =="
{ cat "$WORK/sup.clean"; echo "mart_b|old_name"; } > "$WORK/sup.extra"
run edw.clean sup.extra ds.clean
rc_is extra "$RC" 1
contains extra "$OUT" "stale in the dataset, the EDW dropped/renamed: old_name"
contains extra "$OUT" "✗ mart_b (dataset 2):"

echo
echo "== a dataset that exposes NO columns at all is caught, not skipped =="
# mart_c exists in the EDW and is registered, but has no rows in table_columns:
# the column join would drop it, so the gate must compare against the dataset list.
{ cat "$WORK/edw.clean"; echo "mart_c|id"; } > "$WORK/edw.c"
{ cat "$WORK/ds.clean";  echo "mart_c|3"; }  > "$WORK/ds.c"
run edw.c sup.clean ds.c
rc_is empty-dataset "$RC" 1
contains empty-dataset "$OUT" "✗ mart_c (dataset 3):"
contains empty-dataset "$OUT" "not in the dataset, the EDW has: id"
contains empty-dataset "$OUT" "STALE: 1 of 3 dataset(s)"

echo
echo "== a dataset whose table is gone from the EDW =="
{ cat "$WORK/edw.clean"; } > "$WORK/edw.same"
{ cat "$WORK/sup.clean"; echo "mart_z|id"; } > "$WORK/sup.z"
{ cat "$WORK/ds.clean";  echo "mart_z|4"; }  > "$WORK/ds.z"
run edw.same sup.z ds.z
rc_is gone "$RC" 1
contains gone "$OUT" "✗ mart_z (dataset 4): no such table in the EDW any more"

echo
echo "== nothing to compare (a virgin instance) must not be a false failure =="
: > "$WORK/ds.empty"
: > "$WORK/sup.empty"
run edw.same sup.empty ds.empty
rc_is virgin "$RC" 0
contains virgin "$OUT" "SKIP: no Superset dataset in 'mart' yet — nothing to compare"

echo
echo "== an unreadable database must fail, never pass =="
mkdir -p "$WORK/stub"
printf '#!/usr/bin/env bash\nexit 1\n' > "$WORK/stub/docker"
chmod +x "$WORK/stub/docker"
OUT="$(PATH="$WORK/stub:$PATH" bash "$GATE" 2>&1)"; RC=$?
rc_is unreadable "$RC" 2
contains unreadable "$OUT" "✗ CANNOT READ the EDW, schema mart"

echo
echo "== wiring: the gate is actually run by the seed verifier, the e2e and install =="
ROOT="$(cd "$H/.." && pwd)"
grep -qF 'verify_dataset_metadata.sh' "$ROOT/superset/verify_seed.sh" \
  && ok "verify_seed.sh runs the gate" || bad "verify_seed.sh does not run the gate"
grep -qF 'verify_dataset_metadata.sh' "$ROOT/e2e-testing/full-cycle.sh" \
  && ok "full-cycle.sh phase 8 runs the gate" || bad "full-cycle.sh does not run the gate"
grep -qF 'verify_dataset_metadata.sh' "$ROOT/install.sh" \
  && ok "install.sh verify_superset runs the gate" || bad "install.sh does not run the gate"
grep -qF '_superset_dataset_metadata.py' "$ROOT/init.sh" \
  && ok "init.sh copies the shared helper into _conf" || bad "init.sh does not copy the helper"
for f in setup.py create_missing_dashboards.py create_grocery_ops_dashboard.py create_data_quality_dashboard.py; do
  grep -qF '_superset_dataset_metadata' "$ROOT/superset/$f" \
    && ok "$f refreshes its datasets" || bad "$f never refreshes its datasets"
done

echo
echo "test-dataset-metadata-gate.sh: $PASS passed, $FAIL failed"
[ "$FAIL" = "0" ] || exit 1
