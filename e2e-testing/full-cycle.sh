#!/usr/bin/env bash
# full-cycle.sh — End-to-end fresh-instance test for the data-lab stack.
#
# PURPOSE: Prove the entire platform reproduces correctly from a wiped state:
#   stop → wipe conf → reseed (init.sh) → start → verisim backfill →
#   full pipeline (ingest + dbt) → superset seed → automated verification.
#
# STRICT RULE (Chris, 2026-08-31): any manual/live-DB fix made while this is
# running means the test has FAILED. Fix the repo/process instead, commit,
# and RESTART this script from the top. A pass is only valid on an untouched
# run of the checked-out code.
#
# Usage:
#   bash full-cycle.sh              # full wipe + cycle
#   bash full-cycle.sh --no-wipe    # reuse existing _conf (start+pipeline+verify only)
#   bash full-cycle.sh --verify     # run verification phases only
#
# Exit 0 = every phase passed with no manual intervention. Exit 1 = failed
# (see LOG_FILE for the failing phase).

set -uo pipefail

DATALAB="${DATALAB:-/opt/data-lab}"
IP="${IP:-192.168.1.7}"
LOG_DIR="${LOG_DIR:-/tmp/e2e-full-cycle}"
LOG_FILE="$LOG_DIR/full-cycle-$(date +%Y%m%d-%H%M%S).log"
RUN_TAG="manual_fullcycle_$(date +%s)"
WIPE=true
VERIFY_ONLY=false

for arg in "$@"; do
  case "$arg" in
    --no-wipe)  WIPE=false ;;
    --verify)   VERIFY_ONLY=true; WIPE=false ;;
    *) echo "unknown arg: $arg"; exit 2 ;;
  esac
done

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

FAILED=""
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; FAILED="$FAILED $1"; }

# --- helpers ---------------------------------------------------------------

airflow_token() {
  curl -s -X POST "http://localhost:8080/auth/token" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}' |
    python3 -c 'import sys,json; print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null
}

wait_dag_run() {  # wait_dag_run <dag_id> <run_id> <max_minutes>
  local dag=$1 run=$2 max=$3 elapsed=0
  while [ $elapsed -lt $((max * 60)) ]; do
    local tok state
    tok=$(airflow_token)
    state=$(curl -s -H "Authorization: Bearer $tok" \
      "http://localhost:8080/api/v2/dags/$dag/dagRuns/$run" |
      python3 -c 'import sys,json; print(json.load(sys.stdin).get("state","unknown"))' 2>/dev/null)
    echo "  [$dag/$run] state=$state (${elapsed}s)"
    [ "$state" = "success" ] && return 0
    [ "$state" = "failed" ] && return 1
    sleep 30; elapsed=$((elapsed + 30))
  done
  echo "  TIMEOUT after ${max}m"
  return 1
}

# --- phases ---------------------------------------------------------------

phase_stop() {
  echo "=== Phase 1: STOP ==="
  (cd "$DATALAB" && bash stop.sh) || { fail "stop"; return; }
  pass "stop"
}

phase_wipe() {
  echo "=== Phase 2: WIPE conf ==="
  [ "$WIPE" = false ] && { echo "skipped (--no-wipe)"; return; }
  rm -rf "$DATALAB/_conf" || { fail "wipe"; return; }
  pass "wipe"
}

phase_reseed() {
  echo "=== Phase 3: RESEED (generate-secrets + init) ==="
  (cd "$DATALAB" && bash generate-secrets.sh) || { fail "generate-secrets"; return; }
  (cd "$DATALAB" && bash init.sh) || { fail "init"; return; }
  pass "reseed"
}

phase_start() {
  echo "=== Phase 4: START ==="
  (cd "$DATALAB" && bash start.sh --continue-on-error) || fail "start (some stacks)"
  # All key containers must be up
  sleep 15
  local missing=""
  for c in postgres verisim-grocery airflow-apiserver airflow-worker superset; do
    docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true || missing="$missing $c"
  done
  [ -n "$missing" ] && { fail "start containers missing:$missing"; return; }
  pass "start"
}

phase_backfill() {
  echo "=== Phase 5: VERISIM BACKFILL ==="
  local max=15 elapsed=0
  while [ $elapsed -lt $((max * 60)) ]; do
    local n
    n=$(docker exec verisim-grocery psql -U verisim -d grocery -tAc \
        "select count(*) from pos.transactions" 2>/dev/null || echo 0)
    echo "  source txns=$n (${elapsed}s)"
    if [ "${n:-0}" -gt 50000 ]; then pass "backfill ($n txns)"; return; fi
    sleep 30; elapsed=$((elapsed + 30))
  done
  fail "backfill (only ${n:-0} txns after ${max}m)"
}

phase_pipeline() {
  echo "=== Phase 6: PIPELINE (ingest + dbt via Airflow) ==="
  local tok
  tok=$(airflow_token)
  [ -z "$tok" ] && { fail "airflow auth"; return; }
  # Unpause + trigger full pipeline on the empty EDW → full backfill pull
  docker exec airflow-apiserver bash -c '
    airflow dags unpause -y grocery_complete_pipeline >/dev/null 2>&1
    airflow dags unpause -y grocery_dbt >/dev/null 2>&1
    airflow dags unpause -y grocery_ingest_api >/dev/null 2>&1'
  curl -s -X POST "http://localhost:8080/api/v2/dags/grocery_complete_pipeline/dagRuns" \
    -H "Authorization: Bearer $tok" -H "Content-Type: application/json" \
    -d "{\"logical_date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"dag_run_id\": \"$RUN_TAG\"}" \
    -o "$LOG_DIR/trigger.json"
  grep -q '"queued"\|"running"' "$LOG_DIR/trigger.json" || { fail "pipeline trigger"; return; }
  if wait_dag_run grocery_complete_pipeline "$RUN_TAG" 40; then
    pass "pipeline"
  else
    # Fallback: run_marts may be the only failure — surface it clearly, still FAIL
    echo "  last dbt log tail:"
    tail -5 "$DATALAB/airflow/dbt/grocery/logs/dbt.log" 2>/dev/null | tr -d '\000' | sed 's/^/    /'
    fail "pipeline run"
  fi
}

phase_seed_superset() {
  echo "=== Phase 7: SUPERSET SEED ==="
  docker rm superset-setup 2>/dev/null >/dev/null
  docker compose -f "$DATALAB/superset/compose.yaml" up -d --force-recreate superset-setup >/dev/null 2>&1
  local i
  for i in $(seq 1 40); do
    [ "$(docker inspect superset-setup --format '{{.State.Status}}' 2>/dev/null)" = "exited" ] && break
    sleep 10
  done
  local code fails
  code=$(docker inspect superset-setup --format '{{.State.ExitCode}}' 2>/dev/null)
  fails=$(docker logs superset-setup 2>&1 | grep -cE "✗" || true)
  echo "  superset-setup exit=$code, ✗ lines=$fails"
  if [ "$code" = "0" ] && [ "$fails" -eq 0 ]; then pass "superset seed"; else fail "superset seed"; fi
}

phase_verify() {
  echo "=== Phase 8: VERIFY ==="
  # 8a: structural checks (DB-level; see verify_seed.sh for the queries)
  bash "$DATALAB/superset/verify_seed.sh" > "$LOG_DIR/verify_seed.out" 2>&1 || fail "verify_seed.sh"
  grep -E "per-dashboard: 9 = [0-9]+" "$LOG_DIR/verify_seed.out" | tail -1
  local n_dashes n_null_ds n_null_qc
  n_dashes=$(docker exec postgres psql -U postgres -d superset -tAc "select count(*) from dashboards")
  n_null_ds=$(docker exec postgres psql -U postgres -d superset -tAc "select count(*) from slices where datasource_id is null")
  n_null_qc=$(docker exec postgres psql -U postgres -d superset -tAc "select count(*) from slices where query_context is null")
  [ "$n_dashes" -ge 11 ] && pass "11+ dashboards ($n_dashes)" || fail "dashboards ($n_dashes)"
  [ "$n_null_ds" -eq 0 ] && pass "datasource_id all set" || fail "$n_null_ds charts missing datasource_id"
  [ "$n_null_qc" -eq 0 ] && pass "query_context all set" || fail "$n_null_qc charts missing query_context"

  # 8b: data actually flowed through (raw > 0, marts populated, staging fresh)
  local raw mart_tbls
  raw=$(docker exec postgres psql -U postgres -d grocery -tAc "select coalesce(sum(n_live_tup),0) from pg_stat_user_tables where schemaname like 'raw_%'")
  mart_tbls=$(docker exec postgres psql -U postgres -d grocery -tAc "select count(*) from pg_stat_user_tables where schemaname='mart' and n_live_tup>0")
  [ "${raw:-0}" -gt 100000 ] && pass "raw populated ($raw rows)" || fail "raw empty-ish ($raw)"
  [ "${mart_tbls:-0}" -ge 42 ] && pass "42 marts populated" || fail "only $mart_tbls populated marts"

  # 8c: service endpoints
  for svc in "8080/health|airflow" "8088/|superset" "8082/|dbt-docs" "8010/health|verisim-api" "8501/|verisim-ui"; do
    local url name code
    url="http://localhost:${svc%%|*}"; name="${svc##*|}"
    code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
    case "$code" in 2*|3*|404) pass "$name ($code)" ;; *) fail "$name ($code)" ;; esac
  done
  echo "  NOTE: browser DOM verification of all 11 dashboards is the FINAL"
  echo "        gate and is performed by the agent (see skill e2e-testing)."
}

# --- main -----------------------------------------------------------------

echo "full-cycle.sh | tag=$RUN_TAG wipe=$WIPE log=$LOG_FILE"
if [ "$VERIFY_ONLY" = true ]; then
  phase_verify
else
  phase_stop
  phase_wipe
  phase_reseed
  phase_start
  phase_backfill
  phase_pipeline
  phase_seed_superset
  phase_verify
fi

echo
if [ -z "$FAILED" ]; then
  echo "OVERALL: PASS — e2e full cycle clean"
  exit 0
else
  echo "OVERALL: FAIL — failed phases:$FAILED"
  echo "Any fix made during this run invalidates it: fold into repo, commit, RESTART."
  exit 1
fi
