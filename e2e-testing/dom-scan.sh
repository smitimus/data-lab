#!/usr/bin/env bash
# Thin wrapper around dom-scan.js — the browser DOM gate.
#
# Why a wrapper at all: the gate's exit code is what the e2e verdict reads, so the
# things that turn a clean instance into a false PASS have to be checked before
# the scan starts rather than after. This wrapper refuses to run when node or a
# Chromium binary is missing (an absent browser used to mean "0 error tiles"), and
# it logs the run so a pass can be re-read afterwards.
#
# It runs on the WORKSTATION, not on the slot: neither dev (<dev-slot>) nor test
# (<test-slot>) ships node or Chromium. The harness drives the slot's Superset
# over HTTP + headless Chromium locally.
#
# usage: bash e2e-testing/dom-scan.sh [host] [dash[,<dash>...] ...] [--json <path>]
#        bash e2e-testing/dom-scan.sh <test-slot>                 # every dashboard
#        bash e2e-testing/dom-scan.sh <test-slot> data-quality-ops
#
# host defaults to $DOM_SCAN_HOST, then .env's IP, then 127.0.0.1.
# Env: SUPERSET_PORT, SUPERSET_USER/SUPERSET_PASSWORD, CHROMIUM_BIN, CDP_PORT,
#      DOM_SCAN_OUT, LOG_DIR (default /tmp/e2e-test-logs)
# exit codes are dom-scan.js's: 0 clean | 1 error tiles | 2 wrong page | 3 harness
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ENTRY="$HERE/dom-scan.js"
LOG_DIR="${LOG_DIR:-/tmp/e2e-test-logs}"

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  sed -n '2,25p' "$HERE/dom-scan.sh" | sed 's/^# \{0,1\}//'
  echo
  node "$ENTRY" --help
  exit 0
fi

# --- prerequisites ---------------------------------------------------------
if ! command -v node >/dev/null 2>&1; then
  echo "dom-scan: node is not installed on this box — the DOM gate cannot run (exit 3)" >&2
  exit 3
fi
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]')"
if [ "$NODE_MAJOR" -lt 22 ]; then
  echo "dom-scan: node $(node -p process.versions.node) has no global WebSocket — node >= 22 required (exit 3)" >&2
  exit 3
fi

CHROMIUM="${CHROMIUM_BIN:-}"
if [ -z "$CHROMIUM" ]; then
  for c in /usr/sbin/chromium /usr/bin/chromium /usr/bin/chromium-browser /usr/bin/google-chrome; do
    if [ -x "$c" ]; then CHROMIUM="$c"; break; fi
  done
fi
if [ -z "$CHROMIUM" ]; then
  echo "dom-scan: no Chromium binary found (set CHROMIUM_BIN) — an 'empty page' scan is a false PASS, refusing (exit 3)" >&2
  exit 3
fi

# Host: first positional, else $DOM_SCAN_HOST, else .env's IP, else loopback.
HOST="${1:-${DOM_SCAN_HOST:-}}"
ARGS=()
if [ -n "$HOST" ]; then
  shift
  ARGS=("$@")
else
  if [ -f "$HERE/.env" ]; then
    HOST="$(sed -n 's/^ *IP *= *//p' "$HERE/.env" | head -1 | tr -d '"'"'"'')"
  fi
  HOST="${HOST:-127.0.0.1}"
  ARGS=("$@")
fi

mkdir -p "$LOG_DIR" 2>/dev/null || true
LOG="$LOG_DIR/dom-scan-$(date +%Y%m%d-%H%M%S).log"

echo "dom-scan.sh | host=$HOST port=${SUPERSET_PORT:-8088} chromium=$CHROMIUM node=$(node -p process.versions.node)"
echo "dom-scan.sh | log=$LOG"
CHROMIUM_BIN="$CHROMIUM" node "$ENTRY" "$HOST" "${ARGS[@]+"${ARGS[@]}"}" 2>&1 | tee "$LOG"
RC="${PIPESTATUS[0]}"

case "$RC" in
  0) echo "dom-scan.sh | DOM GATE PASS (exit 0) — log $LOG" ;;
  1) echo "dom-scan.sh | DOM GATE FAIL (exit 1) — error tiles found; log $LOG" ;;
  2) echo "dom-scan.sh | DOM GATE FAIL (exit 2) — wrong page / unresolved target, nothing was certified; log $LOG" ;;
  *) echo "dom-scan.sh | DOM GATE INCONCLUSIVE (exit $RC) — harness failure, this is not a pass; log $LOG" ;;
esac
exit "$RC"
