#!/usr/bin/env bash
# Does dbt's view swap survive a dependent view, and does the layer invocation
# order it? (t_b48af51f)
#
# Why this exists. dbt-postgres rebuilds a VIEW as
#     alter view s.x rename to x__dbt_backup;
#     create view s.x as ...;
#     drop view s.x__dbt_backup cascade;
# A view that READS `x` does not get re-pointed — it follows the rename by OID
# onto the backup — so the trailing CASCADE deletes it. `staging.
# stg_pos_transaction_items` reads `stg_pos_products`, so any run in which
# `stg_pos_products` is rebuilt AFTER the dependent, with nothing rebuilding the
# dependent afterwards, leaves the dependent MISSING: 14 tests plus the two
# intermediate models that read it die on `relation "staging.
# stg_pos_transaction_items" does not exist`, retries included (CT107,
# 2026-09-21). grocery_dbt used to build staging as ONE AIRFLOW TASK PER MODEL,
# all in parallel inside a single dag_run, so which of the two finished second
# was a coin flip per cycle; it now builds the layer with ONE `dbt run --select
# staging`, which lets dbt order the models itself.
#
# What this runs (real dbt, real Postgres, scratch schema):
#   1 the defect   — the per-model shape: build the dependency, build the
#                    dependent, then rebuild the dependency in a separate
#                    invocation. Asserts the dependent is GONE afterwards, i.e.
#                    that the old DAG's shape really does cascade it away.
#   2 restore      — rebuild the dependent, so step 1 left the schema usable.
#   3 the fix      — ONE `dbt run --select staging` over the whole layer:
#                    asserts every staging relation still resolves, that dbt
#                    itself completed `stg_pos_products` before
#                    `stg_pos_transaction_items` in that one process, and that
#                    nothing failed.
#   4 swap path    — a SECOND `dbt run --select staging`: now every view is
#                    rebuilt through rename+create+drop-cascade, the exact
#                    condition the .7 cycles died in. Asserts the same.
# Then it drops its scratch schema. The deployed `staging` schema is never
# written to: every model lands in PROBE_SCHEMA (a fresh one per run).
#
# usage: bash test-staging-view-swap.sh [--project-src <dir>] [--keep] [--dry-run]
#   --project-src  dbt project to probe, in PATH-AS-SEEN-BY-THE-CONTAINER terms
#                  (default /opt/airflow/dbt/grocery, the deployed tree). Its
#                  models/ + dbt_project.yml are overlaid on a copy of the
#                  deployed project, so a CANDIDATE tree can be probed without
#                  installing it — and dbt_packages/ still comes from the image.
#   --keep         leave the container-side project copy and the schema in place
#   --dry-run      print the plan and exit (no dbt, no schema)
#
# Run on the slot host (needs docker + the `postgres` and `airflow-worker`
# containers of a data-lab stack, with the raw_* layer already loaded).
set -uo pipefail

PROJECT_SRC="/opt/airflow/dbt/grocery"
DEPLOYED="/opt/airflow/dbt/grocery"
PROFILES_DIR="/opt/airflow/dbt"
KEEP=0
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --project-src) PROJECT_SRC="$2"; shift 2 ;;
    --keep)        KEEP=1; shift ;;
    --dry-run)     DRY_RUN=1; shift ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done

# A fresh schema per run: this suite must never be the reason a slot is dirty,
# and two runs of it may not share a namespace.
PROBE_SCHEMA="${PROBE_SCHEMA:-tb48_probe_$$}"
LOG_DIR="${LOG_DIR:-/tmp/e2e-staging-view-swap}"
LOG_FILE="$LOG_DIR/staging-view-swap-$(date +%Y%m%d-%H%M%S).log"
WORK=""                      # container-side project copy (set in prep)
FAILS=0
CHECKS=0

check() {   # check <label> <0=ok|1=bad>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = 0 ]; then echo "  ok   $1"; else echo "  BAD  $1"; FAILS=$((FAILS + 1)); fi
}
check_eq() {  # check_eq <label> <want> <got>
  if [ "$2" = "$3" ]; then check "$1" 0; else check "$1 == $2 (got: $3)" 1; fi
}
info() { echo "  ..   $*"; }

psql_tAc() {  # psql_tAc <sql>  (grocery EDW, unaligned)
  docker exec postgres psql -U postgres -d grocery -tAc "$1" 2>/dev/null | tr -d '\r'
}
relation() {  # relation <model> -> "<probe schema>.<model>" or MISSING
  psql_tAc "select coalesce(to_regclass('$PROBE_SCHEMA.$1')::text, 'MISSING')"
}
expect_relation() {  # expect_relation <label> <model> <"<schema>.<model>"|MISSING>
  local want="$3"
  [ "$want" != "MISSING" ] && want="$PROBE_SCHEMA.$3"
  check_eq "$1" "$want" "$(relation "$2")"
}
count_in() {  # count_in <pattern> <file> — "0" when nothing matches (grep -c exits 1)
  grep -cE "$1" "$2" 2>/dev/null || true
}
missing_models() {  # every staging model whose relation is not in PROBE_SCHEMA
  psql_tAc "select coalesce(string_agg(m, ' '), '') from unnest(array[$MODEL_SQL]) m
             where to_regclass('$PROBE_SCHEMA.' || m) is null"
}

# This suite leaves a slot exactly as it found it: the scratch schema and the
# container-side project copy go away on every exit path (--keep opts out).
cleanup() {
  if [ "$KEEP" = 1 ]; then
    info "--keep: leaving ${WORK:-<no copy>} and schema $PROBE_SCHEMA in place"
    return
  fi
  psql_tAc "drop schema if exists $PROBE_SCHEMA cascade" >/dev/null
  [ -n "$WORK" ] && docker exec airflow-worker rm -r -f "$WORK" >/dev/null 2>&1
  return 0
}

dbt_run() {  # dbt_run <label> <dbt args…> — real dbt in airflow-worker, logged
  local label="$1"; shift
  echo
  echo "--- $label: dbt $*"
  docker exec airflow-worker bash -lc \
    "cd $WORK/grocery && timeout 900 dbt $* --profiles-dir $PROFILES_DIR --no-use-colors" 2>&1 |
    tee "$LOG_DIR/$label.dbt.log"
}
trap cleanup EXIT

# dbt's own completion order for the last invocation — the ordering evidence, as
# dbt recorded it, not as a log line was grepped. Prints "<unique_id> <status>
# <completed_at>" sorted by completion.
completion_order() {
  docker exec -i airflow-worker python3 - "$WORK/grocery/target_probe/run_results.json" <<'PY'
import json, sys
results = json.load(open(sys.argv[1]))["results"]
def done(r):
    for t in r.get("timing") or []:
        if t.get("name") == "execute" and t.get("completed_at"):
            return t["completed_at"]
    return r["timing"][-1].get("completed_at") or ""
for r in sorted(results, key=done):
    print("{:52s} {:8s} {}".format(r["unique_id"], r["status"], done(r)))
PY
}

# --- plan / preflight ------------------------------------------------------

echo "=== staging view swap (t_b48af51f)"
echo "project-src:  $PROJECT_SRC  (via copy of $DEPLOYED)"
echo "probe schema: $PROBE_SCHEMA"
echo "log:          $LOG_FILE"
mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

RAW_RELS=$(psql_tAc "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
                      where n.nspname ~ '^raw_' and c.relkind = 'r'")
info "raw_* relations in the EDW: ${RAW_RELS:-unreadable}"
if [ "${RAW_RELS:-0}" -lt 1 ]; then
  echo "FAIL  cannot probe: the staging views read raw_* and there are none — load the slot first"
  echo; echo "STAGING VIEW SWAP: FAIL ($FAILS of $CHECKS assertions)"; exit 1
fi
if [ "$DRY_RUN" = 1 ]; then
  info "dry run: prep + 4 dbt invocations + assertions on $PROBE_SCHEMA"
  echo; echo "STAGING VIEW SWAP: DRY RUN (nothing executed)"; exit 0
fi

# --- prep: a private copy of the project, in its own schema -----------------

WORK=$(docker exec airflow-worker mktemp -d /tmp/tb48-view-swap.XXXXXX | tr -d '\r')
echo "container project copy: $WORK/grocery"
docker exec airflow-worker bash -lc "
  set -e
  cp -a $DEPLOYED $WORK/grocery
  # overlay the candidate tree (models/macros/tests/dbt_project.yml); dbt_packages
  # and the image's dbt version stay as installed
  if [ '$PROJECT_SRC' != '$DEPLOYED' ]; then cp -a $PROJECT_SRC/. $WORK/grocery/; fi
  sed -i 's|^      +schema: staging\$|      +schema: $PROBE_SCHEMA|' $WORK/grocery/dbt_project.yml
  sed -i 's|^target-path: \"target\"\$|target-path: \"target_probe\"|'    $WORK/grocery/dbt_project.yml
  chmod -R u+w $WORK/grocery
" 2>&1 | tail -3

# The two edits above are the isolation: a silent no-op would run the probe
# against the DEPLOYED staging schema, so assert they landed.
inschema=$(docker exec airflow-worker grep -c "^      +schema: $PROBE_SCHEMA\$" "$WORK/grocery/dbt_project.yml" | tr -d '\r')
check_eq "prep: staging +schema was retargeted to $PROBE_SCHEMA" "1" "$inschema"
intarget=$(docker exec airflow-worker grep -c '^target-path: "target_probe"$' "$WORK/grocery/dbt_project.yml" | tr -d '\r')
check_eq "prep: target-path is private (no stale manifest)" "1" "$intarget"

MODELS=$(docker exec airflow-worker bash -lc "ls $WORK/grocery/models/staging/*.sql | xargs -n1 basename | sed 's/\.sql\$//'" | tr -d '\r' | sort)
MODEL_SQL=$(printf "'%s'," $MODELS); MODEL_SQL="${MODEL_SQL%,}"
MODEL_COUNT=$(printf '%s\n' $MODELS | grep -c .)
info "staging models in the probed project: $MODEL_COUNT"
check "prep: the probed tree has the staging layer" "$([ "$MODEL_COUNT" -gt 0 ] && echo 0 || echo 1)"

echo
echo "--- step 1: the defect (the old DAG's shape — one dbt invocation per model)"
dbt_run 01-dependency   run --select stg_pos_products
expect_relation "1a the dependency is built" stg_pos_products stg_pos_products
dbt_run 02-dependent    run --select stg_pos_transaction_items
expect_relation "1b the dependent view resolves" stg_pos_transaction_items stg_pos_transaction_items
# --log-level debug so the swap's own third statement is on the record: dbt only
# logs `Applying DROP to: ...__dbt_backup` at debug, and that DROP is the
# CASCADE the dependent dies in.
dbt_run 03-rebuild-dep  run --select stg_pos_products --log-level debug
expect_relation "1c rebuilding the dependency afterwards drops the dependent (the defect)" \
                stg_pos_transaction_items MISSING
expect_relation "1d the dependency itself is still there (it was the DEPENDENT that died)" \
                stg_pos_products stg_pos_products
check "1e dbt's swap ran its rename+create+drop on the dependency" \
      "$([ "$(count_in 'Applying DROP to: .*stg_pos_products__dbt_backup' "$LOG_DIR/03-rebuild-dep.dbt.log")" -ge 1 ] && echo 0 || echo 1)"
info "postgres NOTICE lines naming the cascade in 1c's log: $(count_in 'cascades to view' "$LOG_DIR/03-rebuild-dep.dbt.log") (dbt does not always forward NOTICEs — 1c is the observable effect)"

echo
echo "--- step 2: restore the dependent"
dbt_run 04-restore      run --select stg_pos_transaction_items
expect_relation "2  the dependent resolves again" stg_pos_transaction_items stg_pos_transaction_items

# model.grocery.<name> line number in the completion order, or ""
order_line() { printf '%s\n' "$2" | grep -n "^model\.grocery\.$1 " | head -1 | cut -d: -f1; }

echo
echo "--- step 3: the fix — ONE invocation for the layer"
dbt_run 05-layer-run1   run --select staging
check_eq "3a no staging relation is missing after one layer invocation" "" "$(missing_models)"
expect_relation "3b the dependent among them" stg_pos_transaction_items stg_pos_transaction_items
order1=$(completion_order)
printf '%s\n' "$order1" > "$LOG_DIR/05-layer-run1.order.txt"
dep_line=$(order_line stg_pos_products "$order1")
child_line=$(order_line stg_pos_transaction_items "$order1")
check "3c dbt completed the dependency before the dependent in that run" \
      "$([ -n "$dep_line" ] && [ -n "$child_line" ] && [ "$dep_line" -lt "$child_line" ] && echo 0 || echo 1)"
check_eq "3d every model in that run succeeded" "0" \
         "$(printf '%s\n' "$order1" | grep -cE '^model\.\S+ +(error|fail|skipped)' || true)"
check_eq "3e the invocation covered the whole staging layer" "$MODEL_COUNT" \
         "$(printf '%s\n' "$order1" | grep -c '^model\.' || true)"

echo
echo "--- step 4: the swap path — a second layer invocation rebuilds every view"
dbt_run 06-layer-run2   run --select staging
check_eq "4a no staging relation is missing after the second run (the swap path)" "" \
         "$(missing_models)"
expect_relation "4b the dependent among them" stg_pos_transaction_items stg_pos_transaction_items
order2=$(completion_order)
printf '%s\n' "$order2" > "$LOG_DIR/06-layer-run2.order.txt"
dep_line=$(order_line stg_pos_products "$order2")
child_line=$(order_line stg_pos_transaction_items "$order2")
check "4c dbt completed the dependency before the dependent again" \
      "$([ -n "$dep_line" ] && [ -n "$child_line" ] && [ "$dep_line" -lt "$child_line" ] && echo 0 || echo 1)"
check_eq "4d every model in that run succeeded" "0" \
         "$(printf '%s\n' "$order2" | grep -cE '^model\.\S+ +(error|fail|skipped)' || true)"
check_eq "4e the invocation covered the whole staging layer" "$MODEL_COUNT" \
         "$(printf '%s\n' "$order2" | grep -c '^model\.' || true)"

echo
echo "--- cleanup"
cleanup
if [ "$KEEP" = 0 ]; then
  check_eq "cleanup: the scratch schema is gone" "MISSING" \
           "$(psql_tAc "select coalesce(to_regclass('$PROBE_SCHEMA.stg_pos_products')::text, 'MISSING')")"
fi

echo
if [ "$FAILS" = 0 ]; then
  echo "STAGING VIEW SWAP: PASS ($CHECKS assertions)"
else
  echo "STAGING VIEW SWAP: FAIL ($FAILS of $CHECKS assertions)"
fi
echo "log: $LOG_FILE"
exit $([ "$FAILS" = 0 ] && echo 0 || echo 1)
