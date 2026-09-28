#!/usr/bin/env bash
# Guard tests for dom-scan.js — the browser DOM gate.
#
# Why this exists: the DOM gate is the FINAL gate of the e2e full cycle, and its
# two costly failure modes are both silent. A scan that certifies the WRONG page
# (an expired session landing on /login/, or the SPA settling on a different
# dashboard) reports "0 error tiles" and passes; a scan whose error regex has
# drifted from the frontend's strings reports "0 error tiles" because it detects
# nothing. Both are false PASSes, so both are pinned down here:
#
#   part 1 (offline, no host, no browser): the pure guards — pathOk, verdict,
#     parseArgs — and the error regex, asserted against the strings the frontend
#     actually prints AND against clean tile text (no false positives);
#   part 2 (live, needs the slot): a target that does not exist and an id that
#     does not exist must both exit 2, while a real dashboard must NOT (0 or 1,
#     and landed_on_expected) — so "exit 2" cannot be an always-on harness quirk.
#
# usage: bash test-dom-scan-guard.sh [host]     # host defaults to $DOM_SCAN_HOST
# exit 0 = all assertions hold, 1 = at least one does not.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ENTRY="$HERE/dom-scan.js"
HOST="${1:-${DOM_SCAN_HOST:-}}"
CONTROL="${DOM_SCAN_CONTROL:-data-quality-ops}"

CHECKS=0
FAILS=0
check() {    # check <label> <0|1 result>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = 0 ]; then echo "  ok   $1"; else echo "  BAD  $1"; FAILS=$((FAILS + 1)); fi
}
check_eq() { # check_eq <label> <got> <want>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  BAD  $1 (got '$2', want '$3')"; FAILS=$((FAILS + 1)); fi
}

echo "=== dom-scan guard tests"
echo "harness: $ENTRY"
echo "host   : ${HOST:-<none — live part will be skipped>}"

# --- part 1: the pure guards, offline --------------------------------------
echo
echo "--- part 1: guards + error regex (offline)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT

cat > "$SCRATCH/unit.js" <<'JS'
const m = require(process.argv[2]);
let checks = 0, fails = 0;
const eq = (label, got, want) => {
  checks++;
  if (JSON.stringify(got) === JSON.stringify(want)) console.log(`  ok   ${label}`);
  else { console.log(`  BAD  ${label} (got ${JSON.stringify(got)}, want ${JSON.stringify(want)})`); fails++; }
};

// --- the error regex: a keyword missing here is a false NEGATIVE, not a pass.
// Every string below has been observed on this stack for a broken tile.
const BROKEN = [
  'Unexpected error See more',                                              // dash 1 'Stock Aging Breakdown'
  'Columns missing in dataset',                                             // dash 12 dataset mis-binding
  'Columns missing in datasource',                                          // newer frontend wording
  'Datetime column not provided. Please set it in the chart',               // dateless pie
  'Something went wrong',                                                   // generic backend 500
  'rolling window',                                                         // window-fn params
  'There is no chart definition associated with this component, could it have been deleted? Delete this container',
  'Error: 400 BAD REQUEST',                                                 // chart query failure
];
for (const t of BROKEN) eq(`regex catches: ${t.slice(0, 46)}`, m.ERROR_RE.test(t), true);

// ...and it must NOT fire on tile text that renders fine, or the gate cries wolf.
const CLEAN = [
  'Stock Aging', '$1,545', 'Peak Hours Heatmap', 'Items Below Reorder or Out of Stock',
  'Weekly Revenue by Store', 'Shrink % of Revenue Trend', 'Avg Transaction Count by Hour',
  'No results were returned for this query',   // legit empty chart, not a tile error
];
for (const t of CLEAN) eq(`regex ignores clean tile: ${t.slice(0, 34)}`, m.ERROR_RE.test(t), false);

// --- pathOk: the requested dashboard must be the rendered one --------------
const DQR = { id: 12, slug: 'data-quality-ops' };
const NOSLUG = { id: 3, slug: null };
eq('slug path accepted', m.pathOk('/superset/dashboard/data-quality-ops/', DQR), true);
eq('id path accepted', m.pathOk('/superset/dashboard/12/', DQR), true);
eq('id path without slash accepted', m.pathOk('/superset/dashboard/12', DQR), true);
eq('null slug falls back to id', m.pathOk('/superset/dashboard/3/', NOSLUG), true);
eq('different dashboard refused', m.pathOk('/superset/dashboard/11/', DQR), false);
eq('/login/ refused', m.pathOk('/login/', DQR), false);
eq('id prefix collision refused (1 vs 11)', m.pathOk('/superset/dashboard/1/', { id: 11, slug: 'x' }), false);
eq('dashboard list page refused', m.pathOk('/dashboard/list/', DQR), false);
eq('cross-instance slug refused (dev dash 6 id)', m.pathOk('/superset/dashboard/6/', DQR), false);

// --- verdict: which exit code each outcome earns --------------------------
eq('clean page -> 0', m.verdict({ landed_on_expected: true, errs: 0, empty: false }), 0);
eq('error tiles -> 1', m.verdict({ landed_on_expected: true, errs: 3, empty: false }), 1);
eq('charts expected but none rendered -> 1', m.verdict({ landed_on_expected: true, errs: 0, empty: true }), 1);
eq('wrong page (clean count) -> 2', m.verdict({ landed_on_expected: false, errs: 0, empty: false }), 2);
eq('wrong page with tiles -> 2', m.verdict({ landed_on_expected: false, errs: 7, empty: false }), 2);

// --- parseArgs: the documented invocation forms ---------------------------
const a = m.parseArgs(['192.0.2.7', 'data-quality-ops,store_hr_labor', '4', '--json', '/tmp/r.json']);
eq('host parsed', a.host, '192.0.2.7');
eq('comma and space targets both parsed', a.targets, ['data-quality-ops', 'store_hr_labor', '4']);
eq('--json parsed', a.json, '/tmp/r.json');
const b = m.parseArgs([]);
eq('no host -> host null (usage + exit 3)', b.host, null);
eq('no host -> no targets (scan nothing until a host is given)', b.targets, []);

console.log(`UNIT ${checks - fails}/${checks}`);
process.exit(fails ? 1 : 0);
JS

node "$SCRATCH/unit.js" "$ENTRY"
UNIT_RC=$?
check_eq "offline unit assertions (guards + regex)" "$UNIT_RC" 0

# --- part 2: the exit-2 contract, against a live slot ----------------------
echo
if [ -z "$HOST" ]; then
  echo "--- part 2: live exit-2 contract — SKIPPED (no host given; pass one or set DOM_SCAN_HOST)"
else
  echo "--- part 2: live exit-2 contract (host $HOST)"
  if ! command -v node >/dev/null 2>&1; then
    echo "  BAD  node is not installed on this box"
    FAILS=$((FAILS + 1)); CHECKS=$((CHECKS + 1))
  else
    run_case() { # run_case <label> <expected-rc...> <args...>
      local label="$1"; shift
      local want="$1"; shift
      node "$ENTRY" "$HOST" "$@" >"$SCRATCH/out.txt" 2>&1
      local rc=$?
      local list=" $want "
      if [ "${list#* $rc }" != "$list" ]; then
        echo "  ok   $label (exit $rc)"; else
        echo "  BAD  $label (exit $rc, want one of:$want) — see $SCRATCH/out.txt"; tail -3 "$SCRATCH/out.txt" | sed 's/^/        /'
        FAILS=$((FAILS + 1))
      fi
      CHECKS=$((CHECKS + 1))
    }

    # A target the API cannot resolve must NOT be certified: whatever the browser
    # lands on would be scanned under the wrong name.
    run_case "unresolvable slug -> exit 2" "2" "zz-no-such-dashboard-zz"
    # A resolved-but-nonexistent id lands on a page that is not that dashboard.
    run_case "nonexistent id -> exit 2" "2" "999999"
    # Control: a real dashboard must not exit 2 (0 or 1 depending on its tiles),
    # and must report landed_on_expected — so exit 2 is not an always-on quirk.
    node "$ENTRY" "$HOST" "$CONTROL" --json "$SCRATCH/ctl.json" >"$SCRATCH/ctl.out" 2>&1
    CTL_RC=$?
    echo "  info control '$CONTROL' -> exit $CTL_RC"
    check "$CONTROL exits 0 or 1, not 2/3" "$([ "$CTL_RC" -le 1 ] && echo 0 || echo 1)"
    LANDED="$(node -e "try{const r=require('$SCRATCH/ctl.json');process.stdout.write(String(r.dashboards[0].landed_on_expected))}catch(e){process.stdout.write('?')}")"
    check_eq "control landed on the requested dashboard" "$LANDED" "true"
  fi
fi

echo
if [ "$FAILS" -eq 0 ]; then
  echo "DOM SCAN GUARD TESTS: PASS ($CHECKS assertions)"
  exit 0
fi
echo "DOM SCAN GUARD TESTS: FAIL ($FAILS of $CHECKS assertions)"
exit 1
