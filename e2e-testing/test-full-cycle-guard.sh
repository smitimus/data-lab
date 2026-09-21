#!/usr/bin/env bash
# Offline tests for the full-cycle.sh "at-rest" guard.
#
# Only the two docker calls the guard makes (the dag_run query and the
# superset-setup inspect) are stubbed; every other docker call falls through to
# the real binary, so the script's real control flow is exercised. No Airflow
# state is written: the stub is the only thing that lies about it.
#
# usage: bash run-guard-tests.sh [path/to/full-cycle.sh]
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${1:-$HERE/full-cycle.sh}"
STUB="$HERE/stub"
LOGS="$HERE/logs"
rm -rf "$STUB" "$LOGS"; mkdir -p "$STUB" "$LOGS"

export REAL_DOCKER="$(command -v -p docker)"

cat > "$STUB/docker" <<'EOS'
#!/usr/bin/env bash
# FC_TEST_MODE: atrest | inflight | inflight-dbt | seeding | nodb
MODE="${FC_TEST_MODE:-atrest}"
case "$1" in
  exec)
    case "$*" in
      *dag_run*)
        case "$MODE" in
          nodb)         echo "psql: error: could not connect to server" >&2; exit 2 ;;
          inflight)     printf '%s\n' "grocery_complete_pipeline manual_fullcycle_1789958717 running" "" ;;
          inflight-dbt) printf '%s\n' "grocery_dbt scheduled__2026-09-21T00:00:00+00:00 queued" "grocery_dbt_extra some_other_dag running" ;;
          *)            printf '%s\n' "" ;;
        esac
        exit 0 ;;
    esac ;;
  inspect)
    if [ "${2:-}" = "superset-setup" ]; then
      case "$MODE" in
        seeding) echo "running"; exit 0 ;;
        *)       echo "exited";  exit 0 ;;
      esac
    fi ;;
esac
exec "${REAL_DOCKER:-/usr/bin/docker}" "$@"
EOS
chmod +x "$STUB/docker"
echo "stub installed: $STUB/docker (real docker = $REAL_DOCKER)"

FAILS=0
CHECKS=0
check() {  # check <label> <condition-result>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = 0 ]; then echo "  ok   $1"; else echo "  BAD  $1"; FAILS=$((FAILS + 1)); fi
}

grep_log() {  # grep_log <logfile> <substring> -> 0 if present
  grep -qF "$2" "$1"
}

run_case() {  # run_case <name> <mode> <want_exit> <expect_substr> <forbid_substr>
  local name=$1 mode=$2 want=$3 expect=$4 forbid=$5 out rc
  echo
  echo "=========== case: $name (FC_TEST_MODE=$mode, expecting exit $want)"
  out=$(FC_TEST_MODE="$mode" PATH="$STUB:$PATH" LOG_DIR="$LOGS/$name" \
        timeout 900 bash "$SCRIPT" --verify 2>&1)
  rc=$?
  echo "$out" | grep -vE '^\s*$' | tail -30 | sed 's/^/    | /'
  echo "  --- exit=$rc"
  if [ "$want" = "not3" ]; then [ "$rc" != 3 ]; else [ "$rc" = "$want" ]; fi
  check "exit code = $want" $?
  [ -n "$expect" ] && { printf '%s' "$out" | grep -qF "$expect"; check "output contains '$expect'" $?; }
  if [ -n "$forbid" ]; then
    if printf '%s' "$out" | grep -qF "$forbid"; then check "output must NOT contain '$forbid'" 1
    else check "output must NOT contain '$forbid'" 0; fi
  fi
}

# 1. the reported incident: umbrella pipeline still running -> refuse, no FAILs
run_case "pipeline-in-flight" inflight 3 "pipeline in flight — cannot verify" "FAIL"
# 2. a bare dbt run in flight counts too, and an unrelated dag_id must not match
run_case "dbt-in-flight" inflight-dbt 3 "grocery_dbt scheduled__2026-09-21T00:00:00+00:00 queued" "grocery_dbt_extra"
# 3. superset re-seed still writing slices
run_case "superset-seed-in-flight" seeding 3 "superset re-seed in flight — cannot verify" "FAIL"
# 4. state store unreadable -> unknown is not "at rest"
run_case "state-unknown" nodb 3 "pipeline status unknown — cannot verify" "FAIL"
# 5. control: at rest -> guard steps aside and the real gates run (any verdict but 3)
run_case "at-rest-control" atrest not3 "at-rest check: ok" "REFUSED"

# 5b. the mart gate must measure rows, not pg_stat estimates (CTAS-built marts
#     report n_live_tup=0 forever, which produced the bogus "0 populated marts")
CTRL_LOG=$(ls -t "$LOGS"/at-rest-control/full-cycle-*.log 2>/dev/null | head -1)
if [ -n "$CTRL_LOG" ]; then
  echo
  echo "=========== mart-gate measurement (log: $CTRL_LOG)"
  grep_log "$CTRL_LOG" "42 marts populated (42/42 mart relations hold rows)"
  check "mart gate reports real row measurement (42/42)" $?
  if grep_log "$CTRL_LOG" "only 0 populated marts"; then check "no stats-based 0-mart verdict" 1
  else check "no stats-based 0-mart verdict" 0; fi
else
  check "control log present" 1
fi

echo
if [ "$FAILS" = 0 ]; then echo "GUARD TESTS: PASS (5 cases, $CHECKS assertions)"; exit 0
else echo "GUARD TESTS: FAIL ($FAILS of $CHECKS assertions)"; exit 1; fi
