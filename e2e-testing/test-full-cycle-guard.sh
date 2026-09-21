#!/usr/bin/env bash
# Offline tests for the full-cycle.sh "at-rest" guard and its mart measurement.
#
# The guard's own docker calls (its two dag_run queries and the superset-setup
# inspect) are stubbed, and so are — in the empty-* and marts-* cases — the two
# EDW measurements that decide what the mart layer holds, so the recovery
# verdicts and the mart verdicts can be exercised without a running stack and
# without touching a real platform. Every other docker call falls through to the
# real binary, so the script's real control flow is exercised and the at-rest
# control case measures the live instance. No Airflow state is written: the stub
# is the only thing that lies about it.
#
# usage: bash test-full-cycle-guard.sh [path/to/full-cycle.sh]
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
#             | empty-failed-load | empty-load-ok | empty-no-load
#             | marts-empty-rel | marts-partial | marts-unreadable
#
# The guard asks the Airflow metadata db two different questions; they are told
# apart by the state filter the guard uses — the in-flight query asks for
# ('running','queued'), the "last load" one for ('success','failed'). The empty-*
# modes also answer the two EDW measurements (raw n_live_tup sum, mart row
# probe), so an empty-layer verdict can be exercised with no stack running and
# without touching a real platform. The marts-* modes answer the same two
# measurements differently, to pin the mart verdict itself: one stray empty mart
# relation (marts-empty-rel), a partially built set (marts-partial), and a probe
# that read nothing at all (marts-unreadable). Every other call falls through to
# the real docker binary.
MODE="${FC_TEST_MODE:-atrest}"
case "$1" in
  exec)
    case "$*" in
      *dag_run*)
        if [ "$MODE" = "nodb" ]; then
          echo "psql: error: could not connect to server" >&2; exit 2
        fi
        case "$*" in
          *"'success','failed'"*)   # last ended load-dag run
            case "$MODE" in
              empty-failed-load) printf '%s\n' "grocery_ingest_api t7c88f2f9-verify2 failed" ;;
              empty-load-ok)     printf '%s\n' "grocery_ingest_api manual__2026-09-21T04:09:46.161775+00:00 success" ;;
              marts-*)           printf '%s\n' "grocery_ingest_api manual__2026-09-21T04:09:46.161775+00:00 success" ;;
              *)                 printf '%s\n' "" ;;
            esac ;;
          *)                        # in-flight runs
            case "$MODE" in
              inflight)     printf '%s\n' "grocery_complete_pipeline manual_fullcycle_1789958717 running" "" ;;
              inflight-dbt) printf '%s\n' "grocery_dbt scheduled__2026-09-21T00:00:00+00:00 queued" "grocery_dbt_extra some_other_dag running" ;;
              *)            printf '%s\n' "" ;;
            esac ;;
        esac
        exit 0 ;;
      *n_live_tup*)     # raw layer row estimate
        case "$MODE" in
          empty-*) echo "0";       exit 0 ;;
          marts-*) echo "1180000"; exit 0 ;;
        esac ;;
      *query_to_xml*)   # mart row probe
        case "$MODE" in
          empty-*)         echo "0|0|(none)"; exit 0 ;;
          marts-empty-rel) echo "43|42|zz_probe_empty_tmp"; exit 0 ;;
          marts-partial)   echo "43|41|zz_a zz_b"; exit 0 ;;
          marts-unreadable) echo "psql: error: permission denied for schema mart" >&2; exit 2 ;;
        esac ;;
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

# 6. the recovery window this gate could not see (t_77ac6468): the raw layer was
#    dropped by the documented repair and the rebuild has not started, so the
#    last load ended `failed`. Empty layer + a load that did not succeed is
#    "recovering" -> refuse; never a FAIL list about the rows that are missing
#    because the platform is mid-reload.
run_case "empty-raw-last-load-failed" empty-failed-load 3 \
  "raw layer mid-rebuild / last load did not succeed — cannot verify" "FAIL"
# 7. control for case 6: the same empty layer behind a SUCCESSFUL load is a real
#    data finding (the raw layer went missing after it was loaded) -> FAIL, and
#    the gate must not blanket-refuse whenever a layer is empty.
run_case "empty-raw-last-load-succeeded" empty-load-ok not3 "raw empty-ish (0)" "REFUSED"
# 8. no load has ever ended (fresh wipe, pipeline not triggered yet): nothing has
#    loaded this platform, so there is no data verdict to give
run_case "empty-raw-no-load-ever" empty-no-load 3 \
  "no load has succeeded yet — cannot verify" "FAIL"

# 6b. a refusal has to be actionable: it prints the repair (the recovery
#     procedure's step 3), not just the symptom
REC_LOG=$(ls -t "$LOGS"/empty-raw-last-load-failed/full-cycle-*.log 2>/dev/null | head -1)
if [ -n "$REC_LOG" ]; then
  grep_log "$REC_LOG" "airflow dags trigger grocery_complete_pipeline"
  check "recovery refusal prints the re-run command" $?
else
  check "recovery case log present" 1
fi

# 7b. the mart FAIL has to say WHICH state it saw. With the probe answering
#     "0|0|(none)" there are no mart relations at all — a missing mart layer, not
#     a partially built one — and it must not print the unmeasurable "0 of 0"
#     line the probe defect produced (t_0675c2ca).
SEVEN_LOG=$(ls -t "$LOGS"/empty-raw-last-load-succeeded/full-cycle-*.log 2>/dev/null | head -1)
if [ -n "$SEVEN_LOG" ]; then
  grep_log "$SEVEN_LOG" "no mart relations exist at all"
  check "an empty mart schema is reported as a missing mart layer" $?
  if grep_log "$SEVEN_LOG" "0 of 0 populated marts"; then check "no unmeasurable '0 of 0' mart verdict" 1
  else check "no unmeasurable '0 of 0' mart verdict" 0; fi
else
  check "empty-load-ok log present" 1
fi

# 9. a stray EMPTY mart relation must be a NAMED finding, not a dead probe
#    (t_0675c2ca). The probe used to hand query_to_xml's document to xpath():
#    an empty relation answers the empty string, xpath() raises on it,
#    `2>/dev/null` hid that, and the whole measurement collapsed to
#    "0 of 0 populated marts (empty: unknown)" — one empty relation read as
#    "the mart schema does not exist".
run_case "marts-one-empty-relation" marts-empty-rel not3 \
  "42 marts populated (42/43 mart relations hold rows; empty: zz_probe_empty_tmp)" ""
# 10. a partially built mart set names the empty relations in the FAIL
run_case "marts-partial-set" marts-partial not3 \
  "only 41 of 43 populated marts (empty: zz_a zz_b)" ""
# 11. a probe that could not read the schema must not be reported as "no mart
#     relations exist" (or as a count): nothing was measured
run_case "marts-probe-unreadable" marts-unreadable not3 \
  "the mart probe returned nothing" "no mart relations exist at all"

echo
if [ "$FAILS" = 0 ]; then echo "GUARD TESTS: PASS (11 cases, $CHECKS assertions)"; exit 0
else echo "GUARD TESTS: FAIL ($FAILS of $CHECKS assertions)"; exit 1; fi
