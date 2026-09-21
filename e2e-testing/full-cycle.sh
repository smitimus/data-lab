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
#   bash full-cycle.sh --verify     # verification phases only (refuses with
#                                   #  exit 3 while a pipeline run is in flight)
#
# Exit 0 = every phase passed with no manual intervention. Exit 1 = failed
# (see LOG_FILE for the failing phase). Exit 2 = bad usage. Exit 3 = REFUSED:
# the platform was not at rest (pipeline DAG run in flight), so no data verdict
# was produced — see `verify_at_rest` below.

set -uo pipefail

DATALAB="${DATALAB:-/opt/data-lab}"
# Default to THIS host's address, never a hardcoded one: a baked-in IP makes the gate
# probe a different machine and report on the wrong instance (found 2026-09-20 — this
# defaulted to 192.168.1.7, the host being replaced).
IP="${IP:-$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}')}"
LOG_DIR="${LOG_DIR:-/tmp/e2e-full-cycle}"
LOG_FILE="$LOG_DIR/full-cycle-$(date +%Y%m%d-%H%M%S).log"
RUN_TAG="manual_fullcycle_$(date +%s)"
WIPE=true
VERIFY_ONLY=false
# Exit code for "the platform is not at rest — no data verdict is possible".
# Distinct from 0 (pass), 1 (fail) and 2 (usage) so a refusal can never be
# mistaken for a result.
EXIT_CANNOT_VERIFY=3
# DAGs whose in-flight runs invalidate a data verdict. A bare grocery_dbt run
# leaves the marts half-built just as surely as the umbrella pipeline does, and
# grocery_ingest_api is still writing raw_* while it runs.
TRACKED_DAGS="${TRACKED_DAGS:-grocery_complete_pipeline grocery_dbt grocery_ingest_api}"

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

# --- measurements ----------------------------------------------------------
# Marts are built by CREATE TABLE AS, which never feeds the per-table insert
# counters: pg_stat_user_tables.n_live_tup / n_tup_ins stay 0 and
# pg_class.reltuples stays -1 until an explicit ANALYZE. A stats-based count
# therefore reports "0 populated marts" on a perfectly healthy EDW (dev,
# 2026-09-21: 0 reported vs 42 actually populated) — the gate has to look at
# the rows. Bounded probe: `select 1 ... limit 1` is O(1) per relation.

marts_populated() {
  # "<mart relations>|<populated>|<names of the empty ones>"
  docker exec postgres psql -U postgres -d grocery -tAc "
with mart_rels as (
  select c.relname,
         (xpath('/row/n/text()',
                query_to_xml(format('select 1 as n from mart.%I limit 1', c.relname),
                             false, true, '')))[1]::text as probe
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
   where n.nspname = 'mart'
     and c.relkind = 'r'
)
select count(*)::text
       || '|' || (count(*) filter (where probe = '1'))::text
       || '|' || coalesce(string_agg(relname, ' ' order by relname) filter (where probe is null), '(none)')
  from mart_rels;" 2>/dev/null | tr -d '\r'
}

# --- at-rest guard ---------------------------------------------------------
# A data verdict is only meaningful when nothing is mutating the EDW. Marts
# legitimately do not exist until `transform` finishes, so a gate that runs
# mid-pipeline reports the run's own progress back as a data failure — observed
# 2026-09-20: "FAIL only 0 populated marts" + "18 charts missing query_context"
# on a healthy instance, which sent the operator hunting a bug that did not
# exist for ~20 minutes. Refuse (EXIT_CANNOT_VERIFY) instead of inventing one.
#
# Source of truth is the Airflow metadata db (postgres/airflow), not the REST
# API: it stays readable while the API server is busy or down, and it is the
# same store the scheduler consults to decide what is still running.

inflight_dag_runs() {
  # Prints "<dag_id> <run_id> <state>" per in-flight tracked run.
  # rc 0 = read ok (possibly no rows); rc 2 = status UNKNOWN — an unreadable
  # state store is not the same as "at rest", so it must not yield a verdict.
  local rows
  rows=$(docker exec postgres psql -U postgres -d airflow -tAc \
      "select dag_id || ' ' || run_id || ' ' || state from dag_run where state in ('running','queued')" 2>/dev/null) || return 2
  echo "$rows" | grep -E "^($(echo "$TRACKED_DAGS" | tr ' ' '|')) " || true
}

superset_seed_in_flight() {
  # A re-seed rewrites slices (datasource_id/query_context), so mid-seed the
  # chart gates are exactly as meaningless as mid-pipeline mart counts.
  case "$(docker inspect superset-setup --format '{{.State.Status}}' 2>/dev/null)" in
    running|created|restarting) return 0 ;;
    *) return 1 ;;
  esac
}

refuse_verify() {  # refuse_verify <headline> [detail]
  echo
  echo "REFUSED: $1"
  [ -n "${2:-}" ] && echo "  $2"
  echo "  No data verdict was produced — marts and charts are only assessable once"
  echo "  the platform is at rest. Inspect / wait with:"
  echo "      docker exec postgres psql -U postgres -d airflow -tAc \\"
  echo "        \"select dag_id, run_id, state from dag_run where state not in ('success','failed')\""
  echo
  echo "OVERALL: CANNOT VERIFY — $1 (exit $EXIT_CANNOT_VERIFY)"
}

verify_at_rest() {
  # 0 = safe to emit a data verdict; EXIT_CANNOT_VERIFY = refused.
  local runs rc
  runs=$(inflight_dag_runs); rc=$?
  if [ "$rc" -eq 2 ]; then
    refuse_verify "pipeline status unknown — cannot verify" \
      "postgres/airflow dag_run is unreadable (metadata db or container down)"
    return "$EXIT_CANNOT_VERIFY"
  fi
  if [ -n "$runs" ]; then
    echo "$runs" | sed 's/^/      in flight: /'
    refuse_verify "pipeline in flight — cannot verify" \
      "$(printf '%s\n' "$runs" | grep -c .) run(s) running/queued for: $(echo "$TRACKED_DAGS" | tr ' ' ', ')"
    return "$EXIT_CANNOT_VERIFY"
  fi
  if superset_seed_in_flight; then
    refuse_verify "superset re-seed in flight — cannot verify" \
      "container superset-setup has not finished (dashboard slices still being rewritten)"
    return "$EXIT_CANNOT_VERIFY"
  fi
  echo "  at-rest check: ok (no tracked pipeline DAG run in flight)"
  return 0
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
  # 8-pre: never emit a data verdict for a platform that is still moving
  verify_at_rest || exit "$EXIT_CANNOT_VERIFY"
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

  # 8b: data actually flowed through (raw loaded, mart layer populated)
  # raw_* is loaded by ingest COPY/INSERT, which IS counted by n_live_tup, so the
  # estimate is usable there (9,429,746 estimate vs 9,431,972 exact on dev,
  # 0.02% off a 100k threshold). Marts are CTAS-built and need the row probe.
  local raw marts m_total m_pop m_empty
  raw=$(docker exec postgres psql -U postgres -d grocery -tAc "select coalesce(sum(n_live_tup),0) from pg_stat_user_tables where schemaname like 'raw_%'")
  marts=$(marts_populated)
  IFS='|' read -r m_total m_pop m_empty <<<"$marts"
  [ "${raw:-0}" -gt 100000 ] && pass "raw populated ($raw rows)" || fail "raw empty-ish ($raw)"
  if [ "${m_pop:-0}" -ge 42 ]; then
    pass "42 marts populated ($m_pop/${m_total:-0} mart relations hold rows)"
  else
    fail "only ${m_pop:-0} of ${m_total:-0} populated marts (empty: ${m_empty:-unknown})"
  fi

  # 8b-ii: the EDW must not hold MORE rows than the source it was loaded from.
  # A row-count floor is not a data-correctness check: on 2026-09-21 the ingest
  # was reading the *previous* host's Verisim through a stale IP and loaded
  # 1,136,360 foreign transactions against a 98,112-row source, and every check
  # above still passed (t_05b48b69). The full per-table invariant runs in the
  # ingest DAG (verify_raw_vs_source); these are the load-bearing tables.
  check_no_excess() {  # check_no_excess <raw relation> <source relation>
    local r s
    r=$(docker exec postgres psql -U postgres -d grocery -tAc "select count(*) from $1" 2>/dev/null | tr -d ' ')
    s=$(docker exec verisim-grocery psql -U verisim -d grocery -tAc "select count(*) from $2" 2>/dev/null | tr -d ' ')
    if [ -z "$r" ] || [ -z "$s" ]; then fail "parity $1 vs $2 (unreadable)"; return; fi
    echo "  parity $1=$r <= $2=$s"
    if [ "$r" -gt "$s" ]; then
      fail "parity $1 vs $2 ($r raw > $s source)"
    else
      pass "parity $2 ($r of $s rows)"
    fi
  }
  check_no_excess raw_pos.transactions pos.transactions
  check_no_excess raw_pos.transaction_items pos.transaction_items
  check_no_excess raw_timeclock.events timeclock.events
  check_no_excess raw_hr.employees hr.employees

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
