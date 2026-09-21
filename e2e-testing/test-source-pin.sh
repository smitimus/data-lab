#!/usr/bin/env bash
# The verisim-grocery image pin, checked against the RECORD (off-guest, no docker).
#
# Why this test exists: `smiti/verisim-grocery:latest` is not a version. On 2026-09-21
# dev (106) and test (107) ran that same tag with different bytes — dev a 07:45 build
# that serves online.orders/order_items/order_events and pos.returns/return_items, test
# the three-week-old 1.3.2 that serves none of them — so 5 of the 32 relations
# grocery_complete_pipeline ingests were unservable, the readiness sensor held the run,
# and from outside it read as a hang (t_9f544379). The same class had already cost us a
# minio `:latest` namespace that went unpullable.
#
# The fix is one line in verisim-grocery/compose.yaml: pin the image by digest (or a
# dated tag), so the record alone decides which code a slot runs and a fresh slot either
# gets those exact bytes or fails loudly. infra's app-layer.sh step 6b is what fetches
# and verifies it; this test is what stops the pin being quietly turned back into a
# floating tag by a later edit.
#
# usage: bash test-source-pin.sh [path/to/data-lab]
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="${1:-$(cd "$HERE/.." && pwd)}"
RECORD="$ROOT/verisim-grocery/compose.yaml"
SOURCE_SERVICE="verisim-grocery"

FAILS=0
CHECKS=0
check() {    # check <label> <result: 0 = ok>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = 0 ]; then echo "  ok   $1"; else echo "  BAD  $1"; FAILS=$((FAILS + 1)); fi
}
check_eq() { # check_eq <label> <got> <want>
  CHECKS=$((CHECKS + 1))
  if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  BAD  $1 (got '$2', want '$3')"; FAILS=$((FAILS + 1)); fi
}

image_line() { # path -> the `image:` value of the file's first service
  grep -m1 -E '^[[:space:]]*image:' "$1" 2>/dev/null | sed -E 's/^[[:space:]]*image:[[:space:]]*//; s/[[:space:]]*$//'
}

echo "=== verisim-grocery source pin"
echo "record: $RECORD"
echo

if [ ! -f "$RECORD" ]; then
  check "$RECORD exists" 1
  echo
  echo "SOURCE PIN TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
check "$RECORD exists" 0

REF="$(image_line "$RECORD")"
echo "  pinned reference: ${REF:-<none>}"

# --- 1. the reference must name immutable bytes ------------------------------
# a digest is immutable by construction; a versioned tag is acceptable because
# publishing under it is a release; `latest` (and the other rolling names) are not.
case "$REF" in
  *@sha256:*)
    DIGEST="${REF#*@}"
    case "$DIGEST" in
      sha256:*) check "the record pins by digest ($DIGEST)" 0 ;;
      *)        check "the record's digest is a sha256:… value (got '$DIGEST')" 1 ;;
    esac
    check_eq "the digest is 64 hex characters" \
      "$(printf '%s' "${DIGEST#sha256:}" | tr -cd '0-9a-f' | wc -c | tr -d ' ')" "64"
    ;;
  *:latest|*:stable|*:main|*:master|*:edge|*:dev|*:nightly)
    check "the record pins $SOURCE_SERVICE to a floating tag ('$REF') — a tag is not a version" 1
    echo "       dev and test came apart on exactly this: same tag, different bytes, 5 stalled"
    echo "       ingest tasks. Pin a digest (repo@sha256:…) or a dated/versioned tag."
    ;;
  *)
    check "the record pins $SOURCE_SERVICE to a named tag ('$REF')" 0
    echo "       a versioned tag is a release; keep it moving only by publishing, never by retagging"
    ;;
esac

# --- 2. `compose up` must not chase a manifest the host already satisfies -----
# A digest pin that is only on the fleet (built and side-loaded, never published) is
# still the right pin: the bytes are what matters. `pull_policy: missing` is what keeps
# `compose up` from trying to fetch a registry manifest that does not exist.
if grep -qE '^[[:space:]]*pull_policy:[[:space:]]*missing[[:space:]]*$' "$RECORD"; then
  check "the record sets pull_policy: missing for the pinned stack" 0
else
  check "the record sets pull_policy: missing (so a locally-held digest pin is usable offline)" 1
fi

# --- 3. no other edit may reuse this stack's name for a different image -------
DUPES="$(grep -rlE "^[[:space:]]*container_name:[[:space:]]*${SOURCE_SERVICE}[[:space:]]*$" \
           "$ROOT" --include='compose.yaml' 2>/dev/null | grep -v "^$RECORD$" || true)"
check_eq "only one stack claims container_name: $SOURCE_SERVICE" "${DUPES:-}" ""

# --- 4. report the other floating tags (same class, other stacks) -------------
# Not a failure: these are upstream images whose releases are not part of a data
# contract. Printed because the class has bitten twice and the list is what a
# future pinning pass would start from.
echo
echo "  other stacks, for the record:"
FLOATING=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  r="$(image_line "$f")"
  case "$r" in
    *:latest) printf '    floating  %-34s %s\n' "${f#$ROOT/}" "$r"; FLOATING=$((FLOATING + 1)) ;;
    *)        printf '    pinned    %-34s %s\n' "${f#$ROOT/}" "$r" ;;
  esac
done < <(find "$ROOT" -name 'compose.yaml' | sort)
echo "    ($FLOATING stack(s) still on a floating tag — verisim-grocery's own pin is checked above)"

echo
if [ "$FAILS" = 0 ]; then
  echo "SOURCE PIN TESTS: PASS ($CHECKS assertions)"
  exit 0
else
  echo "SOURCE PIN TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
