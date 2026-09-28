#!/usr/bin/env bash
# Offline tests for the full-cycle.sh phase-6 slot drain (t_67bb60ef).
#
# Why the drain exists: unpausing grocery_complete_pipeline on a freshly reseeded
# metadata db makes the scheduler create a catch-up run for the past schedule
# slot, and the DAG is max_active_runs=1 — so the cycle's own run just queues
# behind it while the catch-up run does the same ingest+dbt. grocery_dbt carries
# retries=1, so a failed task of that run comes back while the cycle's own run is
# in transform: two dbt runs drop/recreate staging relations under each other and
# the cycle fails with `relation staging.stg_pos_transaction_items does not
# exist` (20:58:13 UTC, t_0f50c0ab's cycle). The drain must therefore (a) wait for
# the slot to be empty, (b) not trust a single empty reading — the scheduler
# creates the catch-up run seconds after the unpause, and (c) report what it
# drained by run_id.
#
# What this test does: the drain's only outside dependency is the metadata-db
# read (inflight_dag_runs → `docker exec postgres psql … dag_run`). That call is
# stubbed with a scripted sequence of answers; the functions under test are
# EXTRACTED FROM THE SCRIPT (sed ranges), never re-typed here, so the test
# measures the shipped bytes. No Airflow state is written and no slot is touched.
#
# usage: bash test-full-cycle-drain.sh [path/to/full-cycle.sh]
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${1:-$HERE/full-cycle.sh}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

FAILS=0
CHECKS=0
check() {   # check <label> <condition-result>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = 0 ]; then echo "  ok   $1"; else echo "  BAD  $1"; FAILS=$((FAILS + 1)); fi
}

echo "=== full-cycle.sh phase-6 slot drain"
echo "script: $SCRIPT"

# --- the shipped bytes ------------------------------------------------------
{
  sed -n '/^TRACKED_DAGS=/p'          "$SCRIPT"   # the reader's own dag list
  sed -n '/^inflight_dag_runs() {/,/^}/p' "$SCRIPT"
  sed -n '/^# >>> phase-6 slot drain/,/^# <<< phase-6 slot drain/p' "$SCRIPT"
} > "$WORK/fns.sh"

for want in inflight_dag_runs drain_slot run_rows competing_dag_runs; do
  if ! grep -q "^$want() {" "$WORK/fns.sh"; then
    check "$want() is present in $SCRIPT" 1
    echo
    echo "DRAIN TESTS: FAIL ($FAILS of $CHECKS assertions)"
    exit 1
  fi
  check "$want() is present in $SCRIPT" 0
done
# shellcheck disable=SC1090
. "$WORK/fns.sh"
if ! type drain_slot >/dev/null 2>&1; then
  check "the extracted functions are callable in this shell" 1
  echo
  echo "DRAIN TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
check "the extracted functions are callable in this shell" 0

# The defaults above are the shipped ones; shorten them so the cases run in
# seconds. Every case still drives the same loop, the same reader and the same
# summary lines. DRAIN_MAX_MIN is set per case: the "never comes to rest" case is
# the one that is supposed to hit it.
DRAIN_POLL_S=0
DRAIN_SETTLE_S=2
DRAIN_MAX_MIN=2

# --- the stubbed metadata db ------------------------------------------------
mkdir -p "$WORK/stub"
cat > "$WORK/stub/docker" <<'EOS'
#!/usr/bin/env bash
# Answers inflight_dag_runs from a scripted sequence (resp.1, resp.2, …; the
# last answer is sticky), competing_dag_runs from compete.txt (and records the
# SQL it was asked, so the test can assert the shipped filter), and run_rows
# from rows.txt. A response file whose only content is __PSQL_ERROR__ fails the
# read the way a down metadata db does.
state="${FC_STUB_STATE:?FC_STUB_STATE unset}"
case "$*" in
  *"end="*)    cat "$state/rows.txt" 2>/dev/null; exit 0 ;;
  *run_type*)
    printf '%s\n' "$*" >> "$state/compete.log"
    if [ "$(cat "$state/compete.txt" 2>/dev/null)" = "__PSQL_ERROR__" ]; then
      echo "psql: error: could not connect to server: Connection refused" >&2
      exit 2
    fi
    cat "$state/compete.txt" 2>/dev/null; exit 0 ;;
  *dag_run*) ;;
  *) echo "stub docker: unexpected call: $*" >&2; exit 3 ;;
esac
n=$(cat "$state/calls" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$state/calls"
f="$state/resp.$n"
[ -f "$f" ] || f="$(ls "$state"/resp.* | sort -t. -k2 -n | tail -1)"
if [ "$(cat "$f")" = "__PSQL_ERROR__" ]; then
  echo "psql: error: could not connect to server: Connection refused" >&2
  exit 2
fi
cat "$f"
EOS
chmod +x "$WORK/stub/docker"
export PATH="$WORK/stub:$PATH"

CATCHUP="scheduled__2026-09-21T18:00:00+00:00"
DBT_RUN="manual__2026-09-21T21:22:29.507838+00:00"

scenario() {  # scenario <name> -> creates an empty answer dir, prints its path
  local d="$WORK/$1"
  rm -rf "$d"; mkdir -p "$d"
  printf '%s\n' "$d"
}

run_drain() {  # run_drain <name> [args...] -> sets OUT/RC
  export FC_STUB_STATE="$WORK/$1"; shift
  OUT="$(DRAINED_RUN_IDS="" COMPETING_RUNS="" drain_slot "$@" 2>&1)"
  RC=$?
}

# 1. a slot with nothing in flight is reported as exactly that — no invention of
#    a drained run, so a reader can tell the two apart in the log.
D=$(scenario clear); : > "$D/resp.1"; : > "$D/rows.txt"
run_drain clear
check "clean slot: accepted (rc 0)" "$([ "$RC" = 0 ] && echo 0 || echo 1)"
printf '%s' "$OUT" | grep -qF "slot clear — nothing was in flight (clean slot, no run drained)" \
  && check "clean slot: says no run was drained" 0 || check "clean slot: says no run was drained" 1
printf '%s' "$OUT" | grep -qF "drained run_id(s)" \
  && check "clean slot: does not claim a drained run" 1 || check "clean slot: does not claim a drained run" 0

# 2. THE DEFECT: the scheduler's catch-up run (and the dbt run it triggers) are in
#    flight. The drain must wait for both, name them by run_id, and report how
#    each ENDED.
D=$(scenario catchup)
printf '%s\n' "grocery_complete_pipeline $CATCHUP running" > "$D/resp.1"
printf '%s\n' "grocery_complete_pipeline $CATCHUP running" "grocery_dbt $DBT_RUN running" > "$D/resp.2"
printf '%s\n' "grocery_dbt $DBT_RUN running" > "$D/resp.3"
: > "$D/resp.4"
printf '%s\n' "grocery_complete_pipeline $CATCHUP failed end=2026-09-21 17:26:00-04" \
               "grocery_dbt $DBT_RUN failed end=2026-09-21 17:25:40-04" > "$D/rows.txt"
run_drain catchup
check "catch-up run: drained (rc 0)" "$([ "$RC" = 0 ] && echo 0 || echo 1)"
printf '%s' "$OUT" | grep -qF "waiting on grocery_complete_pipeline $CATCHUP running" \
  && check "catch-up run: the pipeline run is reported while it waits" 0 || check "catch-up run: the pipeline run is reported while it waits" 1
printf '%s' "$OUT" | grep -qF "waiting on grocery_dbt $DBT_RUN running" \
  && check "catch-up run: the dbt run it triggers is reported too" 0 || check "catch-up run: the dbt run it triggers is reported too" 1
printf '%s' "$OUT" | grep -qF "slot clear after" \
  && check "catch-up run: reports clearing the slot after the wait" 0 || check "catch-up run: reports clearing the slot after the wait" 1
printf '%s' "$OUT" | grep -qF "drained run_id(s): $CATCHUP $DBT_RUN" \
  && check "catch-up run: both run_ids are listed in the summary" 0 || check "catch-up run: both run_ids are listed in the summary" 1
printf '%s' "$OUT" | grep -qF "grocery_dbt $DBT_RUN failed end=2026-09-21 17:25:40-04" \
  && check "catch-up run: the drained run's FINAL state is reported" 0 || check "catch-up run: the drained run's FINAL state is reported" 1

# 3. THE SETTLE WINDOW: the slot reads empty because the scheduler has not created
#    its catch-up run yet (it appears a few seconds after the unpause). A drain
#    that trusted the first empty reading would hand phase 6 exactly the run it
#    exists to avoid.
D=$(scenario late)
: > "$D/resp.1"                                        # just after the unpause
printf '%s\n' "grocery_complete_pipeline $CATCHUP running" > "$D/resp.2"   # …and there it is
: > "$D/resp.3"
printf '%s\n' "grocery_complete_pipeline $CATCHUP success end=2026-09-21 17:30:00-04" > "$D/rows.txt"
run_drain late
check "late catch-up run: still drained (rc 0)" "$([ "$RC" = 0 ] && echo 0 || echo 1)"
printf '%s' "$OUT" | grep -qF "confirming a clear slot for 2s" \
  && check "late catch-up run: the first empty reading is only a confirmation window" 0 || check "late catch-up run: the first empty reading is only a confirmation window" 1
printf '%s' "$OUT" | grep -qF "waiting on grocery_complete_pipeline $CATCHUP running" \
  && check "late catch-up run: the run that appeared late is still drained" 0 || check "late catch-up run: the run that appeared late is still drained" 1
printf '%s' "$OUT" | grep -qF "drained run_id(s): $CATCHUP" \
  && check "late catch-up run: it is named in the summary" 0 || check "late catch-up run: it is named in the summary" 1

# 4. a slot that never comes to rest fails the phase rather than queueing the
#    cycle's run behind it (DRAIN_MAX_MIN=0 → the first non-empty reading is the
#    deadline).
D=$(scenario stuck)
printf '%s\n' "grocery_dbt $DBT_RUN running" > "$D/resp.1"
: > "$D/rows.txt"
DRAIN_MAX_MIN=0
run_drain stuck
check "stuck slot: gives up (rc 1)" "$([ "$RC" = 1 ] && echo 0 || echo 1)"
printf '%s' "$OUT" | grep -qF "TIMEOUT after 0m — the slot never came to rest" \
  && check "stuck slot: says the slot never came to rest" 0 || check "stuck slot: says the slot never came to rest" 1
printf '%s' "$OUT" | grep -qF "grocery_dbt $DBT_RUN running" \
  && check "stuck slot: names what it was still waiting on" 0 || check "stuck slot: names what it was still waiting on" 1

# 5. an unreadable metadata db must not be mistaken for an empty slot (the same
#    contract the at-rest guard keeps: unknown ≠ at rest).
D=$(scenario nodb)
printf '%s\n' "__PSQL_ERROR__" > "$D/resp.1"
: > "$D/rows.txt"
run_drain nodb
check "unreadable metadata db: gives up (rc 1)" "$([ "$RC" = 1 ] && echo 0 || echo 1)"
printf '%s' "$OUT" | grep -qF "airflow metadata db unreadable" \
  && check "unreadable metadata db: says so" 0 || check "unreadable metadata db: says so" 1

# 6. the reader's exclude argument is the exact run_id, not a substring: while the
#    cycle's own run is in flight, only OTHER runs are competing.
D=$(scenario exclude)
printf '%s\n' "grocery_complete_pipeline manual_fullcycle_1790025357 queued" \
               "grocery_dbt $DBT_RUN running" > "$D/resp.1"
: > "$D/rows.txt"
export FC_STUB_STATE="$D"
OWN="$(inflight_dag_runs manual_fullcycle_1790025357)"
check "exclude: the cycle's own run is not reported as competing" "$(printf '%s' "$OWN" | grep -q "manual_fullcycle_1790025357" && echo 1 || echo 0)"
check "exclude: the other run is still reported" "$(printf '%s' "$OWN" | grep -qF "grocery_dbt $DBT_RUN running" && echo 0 || echo 1)"

# 7. the mid-cycle watcher's reader. A cycle's own ingest/dbt children are NOT
#    competing runs — the first live run of this drain reported its own
#    grocery_dbt child as one (22:41:14, t_67bb60ef), which would print a WARN on
#    every healthy cycle. The filter has to be in the query the metadata db
#    answers (run_type), so the SQL itself is asserted here.
D=$(scenario compete)
printf '%s\n' "grocery_complete_pipeline scheduled__2026-09-21T18:00:00+00:00 running (scheduled)" > "$D/compete.txt"
: > "$D/resp.1"; : > "$D/rows.txt"
export FC_STUB_STATE="$D"
OUT="$(competing_dag_runs manual_fullcycle_1790029737)"; RC=$?
check "competing runs: an answer is accepted (rc 0)" "$([ "$RC" = 0 ] && echo 0 || echo 1)"
printf '%s' "$OUT" | grep -qF "scheduled__2026-09-21T18:00:00+00:00 running (scheduled)" \
  && check "competing runs: a scheduler-created run is reported" 0 || check "competing runs: a scheduler-created run is reported" 1
printf '%s' "$OUT" | grep -qF "operator_triggered" \
  && check "competing runs: an operator-triggered child is not reported" 1 || check "competing runs: an operator-triggered child is not reported" 0
Q=$(cat "$D/compete.log" 2>/dev/null)
printf '%s' "$Q" | grep -qF "run_type is distinct from 'operator_triggered'" \
  && check "competing runs: the SQL filters on run_type (not after the fact)" 0 || check "competing runs: the SQL filters on run_type (not after the fact)" 1
printf '%s' "$Q" | grep -qF "and run_id <> 'manual_fullcycle_1790029737'" \
  && check "competing runs: the SQL excludes the cycle's own run" 0 || check "competing runs: the SQL excludes the cycle's own run" 1
printf '%s' "$Q" | grep -qF "'grocery_complete_pipeline','grocery_dbt','grocery_ingest_api'" \
  && check "competing runs: the SQL is scoped to the tracked DAGs" 0 || check "competing runs: the SQL is scoped to the tracked DAGs" 1

# a slot where nothing competes reads as empty (no invented warning)
D=$(scenario compete-none); : > "$D/compete.txt"; : > "$D/resp.1"; : > "$D/rows.txt"
export FC_STUB_STATE="$D"
OUT="$(competing_dag_runs manual_fullcycle_1790029737)"; RC=$?
check "competing runs: nothing competing → rc 0 and empty" \
  "$([ "$RC" = 0 ] && [ -z "$OUT" ] && echo 0 || echo 1)"

# …and an unreadable metadata db is not silently \"nothing competing\"
D=$(scenario compete-nodb)
printf '%s\n' "__PSQL_ERROR__" > "$D/compete.txt"; : > "$D/resp.1"; : > "$D/rows.txt"
export FC_STUB_STATE="$D"
OUT="$(competing_dag_runs manual_fullcycle_1790029737)"; RC=$?
check "competing runs: unreadable metadata db → rc 2 (unknown, not clean)" \
  "$([ "$RC" = 2 ] && echo 0 || echo 1)"

# 8. structural: phase 6 must drain BEFORE it triggers, and the wait must watch for
#    a competing run instead of only polling its own.
AWK_PHASE=$(awk '
  /^phase_pipeline\(\) \{/ {inp=1}
  inp && /if ! drain_slot; then/ {print "drain"; exit}
' "$SCRIPT")
check "phase_pipeline calls drain_slot" "$([ "$AWK_PHASE" = drain ] && echo 0 || echo 1)"
check "phase_pipeline drains before it triggers the cycle's run" "$(awk '
  /^phase_pipeline\(\) \{/ {inp=1}
  inp && /if ! drain_slot; then/ {d=NR}
  inp && /curl -s -X POST/ {t=NR; print (d && d < t) ? "yes" : "no"; exit}
' "$SCRIPT" | grep -qx yes && echo 0 || echo 1)"
check "wait_dag_run reports a competing run (RACE)" "$(sed -n '/^wait_dag_run() {/,/^}/p' "$SCRIPT" | grep -q 'RACE: ' && echo 0 || echo 1)"
check "wait_dag_run asks competing_dag_runs, not every in-flight run" \
  "$(sed -n '/^wait_dag_run() {/,/^}/p' "$SCRIPT" | grep -q 'competing_dag_runs "\$run"' && echo 0 || echo 1)"
check "phase_pipeline reports the competing runs it saw" "$(grep -qF '$COMPETING_RUNS' "$SCRIPT" && echo 0 || echo 1)"

echo
if [ "$FAILS" = 0 ]; then
  echo "DRAIN TESTS: PASS ($CHECKS assertions)"
  exit 0
else
  echo "DRAIN TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
