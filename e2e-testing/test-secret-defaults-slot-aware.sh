#!/usr/bin/env bash
# Offline tests for the SLOT-AWARENESS of e2e-testing/test-secret-defaults.sh
# (t_5807e8f9).
#
# global.env is the one file in the tree that is legitimately host-local: on a
# live slot the working tree carries that host's rotated live secrets and its own
# IP / HOMEPAGE_ALLOWED_HOSTNAMES, while the committed file carries the shipped
# defaults (the deploy-owned refresh names it SITE_LOCAL and preserves it
# byte-for-byte). The guard asserts the COMMITTED tree, so an in-place run that
# asserted the working tree failed by construction — `FAIL (21 of 42)` on dev/106
# and test/107 at 518523e — which reads as "the refresh broke the slot" and
# invites "repairing" the one file that must never be touched.
#
# These tests build that tree: a git work tree whose index holds the committed
# content and whose working-tree global.env is site-local-dirty the way a live
# slot's is. Then they assert the contract:
#
#   * in place on a slot the guard PASSes (42 assertions) and names the
#     substitution in one note — and writes nothing into the tree it asserts;
#   * forced onto the working tree (SECRET_DEFAULTS_SOURCE=worktree) it still
#     fails loudly, so the guard has not been neutered;
#   * a committed concrete value is still caught (the b18088e red control);
#   * no live value from the slot-shaped file is ever printed: a mismatch is
#     reported as `len=<n> sha256=<16 hex>`;
#   * the pre-existing invocations keep working: a clean checkout, the git-archive
#     tree with no .git, and --root on a tree the script does not live in;
#   * bad usage / bad environment exits 2.
#
#   bash e2e-testing/test-secret-defaults-slot-aware.sh
set -uo pipefail
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$H/.." && pwd)"
GUARD="$ROOT/e2e-testing/test-secret-defaults.sh"
WORK="$H/logs/secret-defaults-slot-aware"
rm -rf "$WORK"; mkdir -p "$WORK"

PASS=0; FAIL=0
ok()  { echo "  PASS  $*"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL  $*"; FAIL=$((FAIL + 1)); }
rc_is()   { if [ "$2" = "$3" ]; then ok "$1: exit $2"; else bad "$1: exit $2 (want $3)"; fi; }
has()     { if printf '%s' "$2" | grep -qF -- "$3"; then ok "$1: $3"; else bad "$1: missing '$3'"; fi; }
lacks()   { if printf '%s' "$2" | grep -qF -- "$3"; then bad "$1: should not say '$3'"; else ok "$1: no '$3'"; fi; }
eq()      { if [ "$2" = "$3" ]; then ok "$1: $2"; else bad "$1: $2 (want $3)"; fi; }

[ -f "$GUARD" ] || { echo "guard not found at $GUARD"; exit 1; }
git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || { echo "these tests build their fixtures from $ROOT's HEAD, which is not a git work tree"; exit 1; }

GITID=(-c user.email=smity@bridgebum.net -c user.name=smitimus)

mk_tree() {  # mk_tree <name> — the committed tree, plus the guard under test
  local d="$WORK/$1"
  mkdir -p "$d"
  git -C "$ROOT" archive HEAD | tar -x -C "$d" || return 1
  cp "$GUARD" "$d/e2e-testing/test-secret-defaults.sh"
  ( cd "$d" && git init -q && git add -A && git "${GITID[@]}" commit -qm "the committed tree" ) >/dev/null 2>&1
  printf '%s' "$d"
}

site_local() {  # site_local <tree> — make its global.env the file a live slot holds
  local d="$1"
  # install.sh's fill_env generates the two key-shaped values on a real install;
  # the committed file ships dummies. All five therefore differ on a live slot.
  sed -i "s|^AIRFLOW_SECRET_KEY=.*|AIRFLOW_SECRET_KEY=$(printf 'synthetic-per-host-shared-secret-0000000000000000' | base64 | tr -d '\n')|" "$d/global.env"
  sed -i "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$(printf 'synthetic-per-host-shared-secret-0000000000000000' | base64 | tr -d '\n')|" "$d/global.env"
  sed -i "s|^SUPERSET_SECRET_KEY=.*|SUPERSET_SECRET_KEY=$(printf 'synthetic-per-host-shared-secret-0000000000000000' | base64 | tr -d '\n')|" "$d/global.env"
  sed -i "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$(printf 'synthetic-slot-fernet-key-32bytes' | base64 | tr -d '\n')|" "$d/global.env"
  sed -i "s|^ENCRYPTION_KEY=.*|ENCRYPTION_KEY=$(printf 'synthetic-slot-dockhand-key-32byt' | base64 | tr -d '\n')|" "$d/global.env"
  sed -i 's|^IP=.*|IP=192.168.3.6|' "$d/global.env"
  sed -i 's|^HOMEPAGE_ALLOWED_HOSTNAMES=.*|HOMEPAGE_ALLOWED_HOSTNAMES=dev|' "$d/global.env"
}

live_values() {  # live_values <tree> — the five site-local values, one per line
  local k
  for k in AIRFLOW_SECRET_KEY AIRFLOW_JWT_SECRET SUPERSET_SECRET_KEY AIRFLOW_FERNET_KEY ENCRYPTION_KEY; do
    sed -n "s/^$k=//p" "$1/global.env" | head -1 | sed 's/[[:space:]]*#.*$//'
  done
}

assert_no_leak() {  # assert_no_leak <label> <text> — no live value may appear in <text>
  local leaked="" v
  while IFS= read -r v; do
    [ -n "$v" ] || continue
    printf '%s' "$2" | grep -qF -- "$v" && leaked="$leaked $v"
  done <<< "$LIVE"
  [ -z "$leaked" ] && ok "$1: no live value printed" || bad "$1: a live value was printed"
}

echo "== fixtures"
CLEAN="$(mk_tree clean)"
SLOT="$(mk_tree slot)"
site_local "$SLOT"
LIVE="$(live_values "$SLOT")"
eq "the slot fixture differs from its committed global.env" \
  "$([ "$(sha256sum "$SLOT/global.env" | cut -d' ' -f1)" != "$(git -C "$SLOT" show ':./global.env' | sha256sum | cut -d' ' -f1)" ] && echo differs || echo same)" differs
eq "the slot fixture is dirty at global.env only" \
  "$(git -C "$SLOT" status --porcelain | tr -d ' ' | tr '\n' '|')" "Mglobal.env|"
COMMITTED_SHA="$(git -C "$SLOT" show ':./global.env' | sha256sum | cut -d' ' -f1)"
echo

echo "== 1. a clean checkout: unchanged — 42 assertions, no note"
out="$(cd "$CLEAN" && bash e2e-testing/test-secret-defaults.sh 2>&1)"; rc=$?
rc_is "clean checkout" "$rc" 0
has "clean checkout" "$out" "SECRET-DEFAULT TESTS: PASS (42 assertions)"
lacks "clean checkout" "$out" "note:"
echo

echo "== 2. in place on the slot-shaped tree: PASS, and the substitution is named"
out="$(cd "$SLOT" && bash e2e-testing/test-secret-defaults.sh 2>&1)"; rc=$?
rc_is "in place (auto)" "$rc" 0
has "in place (auto)" "$out" "SECRET-DEFAULT TESTS: PASS (42 assertions)"
has "in place (auto)" "$out" "global.env: the COMMITTED copy (git index)"
has "in place (auto)" "$out" "note: $SLOT/global.env differs from the committed one"
has "in place (auto)" "$out" "committed     sha256 $COMMITTED_SHA"
has "in place (auto)" "$out" "SECRET_DEFAULTS_SOURCE=worktree"
assert_no_leak "in place (auto)" "$out"
echo

echo "== 3. nothing is written into the tree it asserts"
before="$(git -C "$SLOT" status --porcelain --untracked-files=all | sort)"
( cd "$SLOT" && bash e2e-testing/test-secret-defaults.sh ) >/dev/null 2>&1
after="$(git -C "$SLOT" status --porcelain --untracked-files=all | sort)"
eq "the slot tree is untouched by the run" "$after" "$before"
echo

echo "== 4. the escape hatch: forced onto the working tree it still fails loudly"
out="$(cd "$SLOT" && SECRET_DEFAULTS_SOURCE=worktree bash e2e-testing/test-secret-defaults.sh 2>&1)"; rc=$?
rc_is "worktree mode" "$rc" 1
has "worktree mode" "$out" "global.env: the working tree ($SLOT/global.env) — forced by SECRET_DEFAULTS_SOURCE=worktree"
has "worktree mode" "$out" "BAD  global.env AIRFLOW_SECRET_KEY is the GENERATE_ME_SECRET sentinel"
has "worktree mode" "$out" "BAD  global.env AIRFLOW_JWT_SECRET is the GENERATE_ME_SECRET sentinel"
has "worktree mode" "$out" "BAD  global.env SUPERSET_SECRET_KEY is the GENERATE_ME_SECRET sentinel"
has "worktree mode" "$out" "BAD  install.sh writes AIRFLOW_SECRET_KEY into global.env"
has "worktree mode" "$out" "of 42 assertions)"
assert_no_leak "worktree mode" "$out"
echo

echo "== 5. SECRET_DEFAULTS_SOURCE=committed asserts the committed copy"
out="$(cd "$SLOT" && SECRET_DEFAULTS_SOURCE=committed bash e2e-testing/test-secret-defaults.sh 2>&1)"; rc=$?
rc_is "committed mode" "$rc" 0
has "committed mode" "$out" "global.env: the committed copy (git index) — forced by SECRET_DEFAULTS_SOURCE=committed"
has "committed mode" "$out" "SECRET-DEFAULT TESTS: PASS (42 assertions)"
echo

echo "== 6. the pre-existing invocations still work"
ARCH="$(mktemp -d "$WORK/archive.XXXXXX")"
git -C "$ROOT" archive HEAD | tar -x -C "$ARCH"
cp "$GUARD" "$ARCH/e2e-testing/test-secret-defaults.sh"
out="$(cd "$ARCH" && bash e2e-testing/test-secret-defaults.sh 2>&1)"; rc=$?
rc_is "archived tree, no .git" "$rc" 0
has "archived tree, no .git" "$out" "global.env: the working tree"
has "archived tree, no .git" "$out" "SECRET-DEFAULT TESTS: PASS (42 assertions)"

SLOT2="$(mk_tree slot2)"; site_local "$SLOT2"
out="$(cd "$WORK" && bash "$GUARD" --root "$SLOT2" 2>&1)"; rc=$?
rc_is "--root from outside the tree" "$rc" 0
has "--root from outside the tree" "$out" "tree: $SLOT2"
has "--root from outside the tree" "$out" "global.env: the COMMITTED copy (git index)"
has "--root from outside the tree" "$out" "SECRET-DEFAULT TESTS: PASS (42 assertions)"
echo

echo "== 7. red control: a COMMITTED concrete value is still caught"
RED="$(mk_tree red)"
( cd "$RED" && sed -i 's|^AIRFLOW_SECRET_KEY=.*|AIRFLOW_SECRET_KEY=AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMMNNNNOOOOPPPP|' global.env \
  && git add global.env && git "${GITID[@]}" commit -qm "put a concrete value back (the b18088e shape)" ) >/dev/null 2>&1
out="$(cd "$RED" && bash e2e-testing/test-secret-defaults.sh 2>&1)"; rc=$?
rc_is "committed concrete value" "$rc" 1
has "committed concrete value" "$out" "BAD  global.env AIRFLOW_SECRET_KEY is the GENERATE_ME_SECRET sentinel"
has "committed concrete value" "$out" "len=64 sha256="
lacks "committed concrete value" "$out" "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMMNNNNOOOOPPPP"
echo

echo "== 8. usage / environment errors exit 2"
out="$(bash "$GUARD" --nope 2>&1)"; rc=$?
rc_is "unknown argument" "$rc" 2
has "unknown argument" "$out" "unknown argument '--nope'"
out="$(bash "$GUARD" --root /nonexistent-tree 2>&1)"; rc=$?
rc_is "--root that is not a directory" "$rc" 2
out="$(cd "$ARCH" && SECRET_DEFAULTS_SOURCE=committed bash "$ARCH/e2e-testing/test-secret-defaults.sh" 2>&1)"; rc=$?
rc_is "committed mode with no git tree" "$rc" 2
has "committed mode with no git tree" "$out" "not a git work tree holding global.env"
out="$(cd "$SLOT" && SECRET_DEFAULTS_SOURCE=bogus bash e2e-testing/test-secret-defaults.sh 2>&1)"; rc=$?
rc_is "unknown SECRET_DEFAULTS_SOURCE" "$rc" 2
echo

echo "test-secret-defaults-slot-aware.sh: $PASS passed, $FAIL failed"
[ "$FAIL" = "0" ] || exit 1
