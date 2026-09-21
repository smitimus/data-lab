#!/usr/bin/env bash
# The mart measurement in full-cycle.sh, run against a REAL EDW.
#
# Why this test exists: the probe used to hand query_to_xml's document to
# xpath(). A populated mart relation answers `<row …><n>1</n></row>`, an EMPTY
# relation answers the empty string — and xpath() against that empty string
# RAISES, which the probe's `2>/dev/null` then hid. One stray empty relation in
# the mart schema aborted the entire measurement and the gate reported
# "only 0 of 0 populated marts (empty: unknown)", so a partly built mart set
# read as "the mart schema does not exist" (t_0675c2ca). This test adds exactly
# that stray relation and asserts the probe NAMES it instead of dying.
#
# It measures the SHIPPED bytes: marts_populated() is extracted from the script
# under test and executed here, against the postgres container. Run it on a slot
# with the stack up; it creates and drops mart.zz_probe_empty_tmp itself (the
# only relation it ever touches) and leaves nothing behind.
#
# The canned states — probe read nothing, no mart relations at all, partially
# built set — are covered by test-full-cycle-guard.sh, which runs the whole
# script with a stubbed probe.
#
# usage: bash test-mart-probe.sh [path/to/full-cycle.sh]
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${1:-$HERE/full-cycle.sh}"
SCRATCH="mart.zz_probe_empty_tmp"
SCRATCH_NAME="zz_probe_empty_tmp"

FAILS=0
CHECKS=0
check() {   # check <label> <condition-result>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = 0 ]; then echo "  ok   $1"; else echo "  BAD  $1"; FAILS=$((FAILS + 1)); fi
}
check_eq() { # check_eq <label> <got> <want>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  BAD  $1 (got '$2', want '$3')"; FAILS=$((FAILS + 1)); fi
}

psql_q() { # psql_q <sql> -> rows on one line, "" on any failure
  docker exec postgres psql -U postgres -d grocery -tAc "$1" 2>/dev/null | tr -d '\r'
}

drop_scratch() {
  psql_q "drop table if exists $SCRATCH" >/dev/null 2>&1 || true
}
trap drop_scratch EXIT

echo "=== mart probe against the real EDW"
echo "script : $SCRIPT"
echo "scratch: $SCRATCH (created and dropped by this test)"

# --- the shipped function, executed here -----------------------------------
BODY=$(sed -n '/^marts_populated() {/,/^}/p' "$SCRIPT")
if [ -z "$BODY" ]; then
  check "marts_populated() is present in $SCRIPT" 1
  echo
  echo "MART PROBE TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
check "marts_populated() is present in $SCRIPT" 0
eval "$BODY"
if ! type marts_populated >/dev/null 2>&1; then
  check "marts_populated() is callable in this shell" 1
  echo
  echo "MART PROBE TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
check "marts_populated() is callable in this shell" 0

# A leftover relation from an earlier run would make the deltas meaningless;
# the name belongs to this test, so it is safe to clear.
if [ -n "$(psql_q "select to_regclass('$SCRATCH')")" ]; then
  echo "  note: $SCRATCH existed already (leftover from an earlier run) — dropping it"
  drop_scratch
fi

BASE=$(marts_populated)
if [ -z "$BASE" ]; then
  check "the probe answers on a live EDW (got nothing back)" 1
  echo
  echo "MART PROBE TESTS: FAIL ($FAILS of $CHECKS assertions)"
  echo "  the probe returned nothing — is the stack up on this host?"
  exit 1
fi
check "the probe answers on a live EDW" 0
IFS='|' read -r base_total base_pop base_empty <<<"$BASE"
echo "  baseline (no scratch relation): total=$base_total populated=$base_pop empty=[$base_empty]"

# 1. THE DEFECT: one empty relation in the mart schema. The probe must still
#    answer, must count it in the total, must NOT count it as populated, and
#    must name it.
psql_q "create table $SCRATCH (n int)" >/dev/null
WITH=$(marts_populated)
IFS='|' read -r w_total w_pop w_empty <<<"$WITH"
echo "  with the empty relation      : total=$w_total populated=$w_pop empty=[$w_empty]"
[ -n "$WITH" ]; check "the probe still answers with an empty mart relation present" $?
check_eq "the empty relation is counted in the mart relation total" "$w_total" "$((base_total + 1))"
check_eq "the empty relation is not counted as populated" "$w_pop" "$base_pop"
case "$w_empty" in
  *"$SCRATCH_NAME"*) check "the empty relation is NAMED in the probe's answer" 0 ;;
  *)                 check "the empty relation is NAMED in the probe's answer (empty=[$w_empty])" 1 ;;
esac

# 2. clean-up must restore the measurement exactly (and leave no relation behind)
drop_scratch
AFTER=$(marts_populated)
IFS='|' read -r a_total a_pop a_empty <<<"$AFTER"
echo "  after dropping it            : total=$a_total populated=$a_pop empty=[$a_empty]"
check_eq "the total is back to the baseline" "$a_total" "$base_total"
check_eq "the populated count is back to the baseline" "$a_pop" "$base_pop"
check_eq "the empty-name list is back to the baseline" "$a_empty" "$base_empty"
check_eq "no scratch relation left behind" \
  "$(psql_q "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'mart' and c.relname = '$SCRATCH_NAME'")" "0"

echo
if [ "$FAILS" = 0 ]; then
  echo "MART PROBE TESTS: PASS ($CHECKS assertions)"
  exit 0
else
  echo "MART PROBE TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
