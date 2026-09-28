#!/usr/bin/env bash
# Guard tests for global.env's shipped secret defaults (t_35b04ad9).
#
# Why this exists: on 2026-09-20 (b18088e) global.env gained five concrete
# secret values — AIRFLOW_SECRET_KEY / AIRFLOW_JWT_SECRET /
# SUPERSET_SECRET_KEY (one shared value), AIRFLOW_FERNET_KEY and
# ENCRYPTION_KEY. They went to the public GitHub mirror, and nothing noticed:
#
#   - the mirror's review scanner reads only a batch's *added* lines, and its
#     `secret=` pattern does not match the `*_SECRET_KEY=` / `*_JWT_SECRET=` /
#     `*_FERNET_KEY=` shape, so a batch that does not touch these lines is
#     clean by construction;
#   - generate-secrets.sh::is_missing() saw a non-placeholder value and
#     skipped them ("AIRFLOW_SECRET_KEY already set — skip"), so every host
#     seeded from the tree signed sessions with a published key;
#   - global-env-sync.py then pushed global.env over every service .env, so
#     the published value beat anything a host generated.
#
# The shipped default has to be RECOGNISABLE as one. That is the invariant this
# file pins down, so the next commit that puts a real-looking value back into
# global.env fails here instead of shipping:
#
#   part 1: the five values are exactly the documented defaults — the three
#           string keys are the GENERATE_ME_SECRET sentinel, the two
#           key-shaped ones decode to "data-lab-shipped-default-key-00N";
#   part 2: no committed .env.example carries a concrete value for any of the
#           five (the scanner's blind spot, asserted directly);
#   part 3: the two key-shaped dummies are format-valid — a Fernet key and
#           Dockhand's key must decode to 32 bytes or their container refuses
#           to start, which is exactly why those two cannot be a sentinel;
#   part 4: offline, in a copy of the tree, generate-secrets.sh recognises the
#           sentinel, replaces it with one per-host shared secret, leaves
#           nothing sentinel-shaped in any service .env, and warns about every
#           rotation that destroys stored state — the two key-shaped defaults it
#           leaves alone (stored Airflow connections / Dockhand credentials),
#           and SUPERSET_SECRET_KEY, which it does replace but which Superset
#           also uses to decrypt its stored connection passwords (t_19e41e00);
#   part 5: a red control — a concrete value in global.env is NOT replaced, so
#           part 4 is not vacuous. That is the b18088e shape, reproduced.
#
# Nothing here WRITES to the tree it asserts: the whole test runs on copies in a
# temp dir, so it is safe to run in place on a live slot.
#
# WHICH global.env IS ASSERTED (t_5807e8f9)
#
# global.env is the one file that is legitimately host-local: on a live slot the
# working tree carries that host's rotated live secrets (and its own IP /
# HOMEPAGE_ALLOWED_HOSTNAMES), while the committed file carries the shipped
# defaults. A slot's /opt/data-lab is a deployment target — the deploy-owned
# refresh names global.env SITE_LOCAL and preserves it byte-for-byte — so an
# in-place run that asserted the working tree would fail by construction: part 1
# fails on the site-local values and the failure cascades through the offline
# copies in parts 4 and 6 (5 + 4 + 12 = 21 of 42 assertions), which reads as "the
# refresh broke the slot" and invites "repairing" the one file that must never be
# touched.
#
#   auto (default)  the committed global.env (the git index — what a commit right
#                   now would ship) whenever the working-tree copy differs from
#                   it, with one explicit note naming the substitution. In a clean
#                   checkout the two are the same bytes and nothing changes.
#   worktree        the working tree, as-is — a dev asserting an uncommitted edit.
#   committed       the committed copy, unconditionally (exit 2 outside a git tree).
#
# usage: bash test-secret-defaults.sh [--root <tree>]
# env:   SECRET_DEFAULTS_SOURCE=auto|worktree|committed   (default auto)
#        --root asserts a tree other than the one this script lives in, so a slot
#        can be guarded from outside its own /opt/data-lab with no write into it.
# exit 0 = all assertions hold, 1 = at least one does not, 2 = bad usage/env.
#
# VALUES ARE NEVER PRINTED (t_5807e8f9)
#
# Every assertion that compares one of the five secrets reports a mismatch as
# `len=<n> sha256=<16 hex>` instead of the value (check_eq_secret). On a slot
# those bytes are the host's live keys: the in-place run this suite used to force
# echoed a live SUPERSET_SECRET_KEY into the log — and from there into the card
# that quoted it. Comparisons that are not secret values (counts, key names, the
# synthetic red control) still print what they saw.
set -uo pipefail

die() { echo "test-secret-defaults: $*" >&2; exit 2; }

TREE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --root)    [ -n "${2:-}" ] || die "--root needs a directory"
               TREE="$2"; shift 2 ;;
    --root=*)  TREE="${1#*=}"; shift ;;
    -h|--help) printf 'usage: bash %s [--root <tree>]\nenv:   SECRET_DEFAULTS_SOURCE=auto|worktree|committed (default auto)\n' "$0"
               exit 0 ;;
    *)         die "unknown argument '$1'" ;;
  esac
done

HERE="$(cd "$(dirname "$0")" && pwd)"
if [ -n "$TREE" ]; then
  [ -d "$TREE" ] || die "--root '$TREE' is not a directory"
  ROOT="$(cd "$TREE" && pwd)"
else
  ROOT="$(cd "$HERE/.." && pwd)"
fi
GLOBAL="$ROOT/global.env"
GEN="$ROOT/generate-secrets.sh"
[ -f "$GLOBAL" ] || die "no global.env under $ROOT"
[ -f "$GEN" ]    || die "no generate-secrets.sh under $ROOT"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/committed"

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
redact() {   # redact <value> — how a secret value is reported: length + digest
  printf 'len=%s sha256=%s' \
    "$(printf '%s' "$1" | wc -c | tr -d ' ')" \
    "$(printf '%s' "$1" | sha256sum | cut -c1-16)"
}
check_eq_secret() { # check_eq_secret <label> <got> <want> — same, but never prints the value
  CHECKS=$((CHECKS + 1))
  if [ "$2" = "$3" ]; then
    echo "  ok   $1"
  else
    echo "  BAD  $1 (got $(redact "$2"), want $(redact "$3"))"
    FAILS=$((FAILS + 1))
  fi
}

SENTINEL=GENERATE_ME_SECRET
DUMMY_PREFIX=data-lab-shipped-default-key-
KEYS_STRING="AIRFLOW_SECRET_KEY AIRFLOW_JWT_SECRET SUPERSET_SECRET_KEY"
KEYS_SHAPED="AIRFLOW_FERNET_KEY ENCRYPTION_KEY"

env_value() {   # env_value <file> <key>   (strips an inline comment)
  sed -n "s/^$2=//p" "$1" | head -1 | sed 's/[[:space:]]*#.*$//'
}
b64len() {      # b64len <value> — decoded byte count, empty on bad base64
  printf '%s' "$1" | tr '_-' '/+' | base64 -d 2>/dev/null | wc -c | tr -d ' '
}
b64txt() {      # b64txt <value> — decoded text
  printf '%s' "$1" | tr '_-' '/+' | base64 -d 2>/dev/null
}
sha_of() {      # sha_of <file> — sha256, empty if unreadable
  sha256sum "$1" 2>/dev/null | cut -d' ' -f1
}

# --- which global.env is asserted (t_5807e8f9) ------------------------------
# The rule is in the header. This resolves it and reports it, so the run says
# which bytes it asserted instead of leaving the substitution silent.
SRC_MODE="${SECRET_DEFAULTS_SOURCE:-auto}"
case "$SRC_MODE" in
  auto|worktree|committed) ;;
  *) die "unknown SECRET_DEFAULTS_SOURCE '$SRC_MODE' (auto|worktree|committed)" ;;
esac

COMMITTED_REF=""
committed_globalenv() {   # committed_globalenv <dest>; sets COMMITTED_REF; 1 if unavailable
  git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 1
  # the index first — that is what a commit right now would ship; HEAD covers an
  # unborn branch or a path that is committed but not staged
  if git -C "$ROOT" show ':./global.env' > "$1" 2>/dev/null && [ -s "$1" ]; then
    COMMITTED_REF="git index"; return 0
  fi
  if git -C "$ROOT" show 'HEAD:./global.env' > "$1" 2>/dev/null && [ -s "$1" ]; then
    COMMITTED_REF="HEAD"; return 0
  fi
  rm -f "$1"
  return 1
}

WT_SHA=""
CO_SHA=""
GLOBAL_SOURCE="the working tree ($GLOBAL)"
GLOBAL_NOTE=0
COMMITTED_OK=0
committed_globalenv "$WORK/committed/global.env" && COMMITTED_OK=1

if [ "$SRC_MODE" = worktree ]; then
  GLOBAL_SOURCE="the working tree ($GLOBAL) — forced by SECRET_DEFAULTS_SOURCE=worktree"
elif [ "$COMMITTED_OK" = 1 ]; then
  WT_SHA="$(sha_of "$GLOBAL")"
  CO_SHA="$(sha_of "$WORK/committed/global.env")"
  if [ "$SRC_MODE" = committed ]; then
    GLOBAL="$WORK/committed/global.env"
    GLOBAL_SOURCE="the committed copy ($COMMITTED_REF) — forced by SECRET_DEFAULTS_SOURCE=committed"
  elif [ "$WT_SHA" != "$CO_SHA" ]; then
    GLOBAL="$WORK/committed/global.env"
    GLOBAL_SOURCE="the COMMITTED copy ($COMMITTED_REF) — the working tree differs; see the note below"
    GLOBAL_NOTE=1
  fi
elif [ "$SRC_MODE" = committed ]; then
  die "SECRET_DEFAULTS_SOURCE=committed, but $ROOT is not a git work tree holding global.env"
fi

echo "=== global.env shipped secret defaults"
echo "tree: $ROOT"
echo "global.env: $GLOBAL_SOURCE"
if [ "$GLOBAL_NOTE" = 1 ]; then
  echo
  echo "note: $ROOT/global.env differs from the committed one. On a live slot that is the"
  echo "      site-local copy: it carries the host's rotated live secrets (and the host's"
  echo "      IP / HOMEPAGE_ALLOWED_HOSTNAMES) and must not be touched — a refresh"
  echo "      preserves it byte-for-byte. The shipped-default assertions below read the"
  echo "      committed copy, which is what a commit right now would ship: the tree, not"
  echo "      the host, is what this suite guards."
  echo "        working tree  sha256 $WT_SHA"
  echo "        committed     sha256 $CO_SHA"
  echo "      This is expected on a slot. To assert the working tree instead:"
  echo "        SECRET_DEFAULTS_SOURCE=worktree bash $0"
fi
echo

# --- part 1: the five values are the documented defaults --------------------
echo "part 1 — global.env carries recognisable defaults"
for k in $KEYS_STRING; do
  check_eq_secret "global.env $k is the $SENTINEL sentinel" "$(env_value "$GLOBAL" "$k")" "$SENTINEL"
done
check_eq_secret "AIRFLOW_FERNET_KEY decodes to ${DUMMY_PREFIX}001" \
  "$(b64txt "$(env_value "$GLOBAL" AIRFLOW_FERNET_KEY)")" "${DUMMY_PREFIX}001"
check_eq_secret "ENCRYPTION_KEY decodes to ${DUMMY_PREFIX}002" \
  "$(b64txt "$(env_value "$GLOBAL" ENCRYPTION_KEY)")" "${DUMMY_PREFIX}002"
echo

# --- part 2: no committed template carries a concrete value -----------------
echo "part 2 — no committed .env.example carries a concrete value"
BAD=""
while IFS= read -r f; do
  for k in $KEYS_STRING $KEYS_SHAPED; do
    v="$(env_value "$f" "$k")"
    [ -z "$v" ] && continue
    case "$v" in
      GENERATE_ME_*|YOUR_*|"") continue ;;
    esac
    BAD="$BAD ${f#$ROOT/}:$k"
  done
done < <(find "$ROOT" -name '.env.example' -not -path '*/.git/*' | sort)
check_eq "every template value for the five keys is a placeholder" "$BAD" ""
echo

# --- part 3: the two key-shaped defaults are format-valid -------------------
echo "part 3 — the key-shaped defaults are valid keys (their consumers refuse to start otherwise)"
check_eq "AIRFLOW_FERNET_KEY is 44 chars" \
  "$(printf '%s' "$(env_value "$GLOBAL" AIRFLOW_FERNET_KEY)" | wc -c | tr -d ' ')" 44
check_eq "AIRFLOW_FERNET_KEY decodes to 32 bytes" \
  "$(b64len "$(env_value "$GLOBAL" AIRFLOW_FERNET_KEY)")" 32
check_eq "ENCRYPTION_KEY decodes to 32 bytes" \
  "$(b64len "$(env_value "$GLOBAL" ENCRYPTION_KEY)")" 32
echo

# --- part 4: the generator recognises and replaces the sentinel -------------
echo "part 4 — generate-secrets.sh replaces the sentinel (offline copy of the tree)"
mkdir -p "$WORK/airflow" "$WORK/dockhand"
cp "$GLOBAL" "$GEN" "$ROOT/global-env-sync.py" "$WORK/"
for svc in airflow dockhand; do
  cp "$ROOT/$svc/.env.example" "$ROOT/$svc/compose.yaml" "$WORK/$svc/"
  cp "$WORK/$svc/.env.example" "$WORK/$svc/.env"
done
FERNET_BEFORE="$(env_value "$WORK/global.env" AIRFLOW_FERNET_KEY)"
ENC_BEFORE="$(env_value "$WORK/global.env" ENCRYPTION_KEY)"
( cd "$WORK" && bash generate-secrets.sh ) > "$WORK/generate.log" 2>&1
check "generate-secrets.sh exits 0" $?
for k in $KEYS_STRING; do
  check "the log reports it generated $k" \
    "$(grep -q "Set $k" "$WORK/generate.log" && echo 0 || echo 1)"
done
check "the log warns about every rotation that destroys stored state" \
  "$(grep -q 'still holds the shipped default' "$WORK/generate.log" \
     && grep -q 're-encrypt-secrets' "$WORK/generate.log" \
     && echo 0 || echo 1)"
check "no sentinel left in global.env" \
  "$(grep -q "^AIRFLOW_SECRET_KEY=$SENTINEL$" "$WORK/global.env" && echo 1 || echo 0)"
check "no sentinel left in any service .env" \
  "$(grep -rq "^AIRFLOW_SECRET_KEY=$SENTINEL$\|^AIRFLOW_JWT_SECRET=$SENTINEL$\|^SUPERSET_SECRET_KEY=$SENTINEL$" \
      --include='.env' "$WORK" && echo 1 || echo 0)"
SHARED="$(env_value "$WORK/global.env" AIRFLOW_SECRET_KEY)"
check_eq_secret "AIRFLOW_JWT_SECRET shares the generated secret" "$(env_value "$WORK/global.env" AIRFLOW_JWT_SECRET)" "$SHARED"
check_eq_secret "SUPERSET_SECRET_KEY shares the generated secret" "$(env_value "$WORK/global.env" SUPERSET_SECRET_KEY)" "$SHARED"
check_eq_secret "the shared secret reached airflow/.env" "$(env_value "$WORK/airflow/.env" AIRFLOW_SECRET_KEY)" "$SHARED"
check_eq_secret "the shared secret reached dockhand/.env" "$(env_value "$WORK/dockhand/.env" SUPERSET_SECRET_KEY)" "$SHARED"
check_eq_secret "AIRFLOW_FERNET_KEY was left alone" "$(env_value "$WORK/global.env" AIRFLOW_FERNET_KEY)" "$FERNET_BEFORE"
check_eq_secret "ENCRYPTION_KEY was left alone" "$(env_value "$WORK/global.env" ENCRYPTION_KEY)" "$ENC_BEFORE"
SUM_BEFORE="$(sed -n 's/^\(AIRFLOW_[A-Z_]*\|SUPERSET_SECRET_KEY\|ENCRYPTION_KEY\)=.*/&/p' "$WORK/global.env" | md5sum)"
( cd "$WORK" && bash generate-secrets.sh ) > /dev/null 2>&1
SUM_AFTER="$(sed -n 's/^\(AIRFLOW_[A-Z_]*\|SUPERSET_SECRET_KEY\|ENCRYPTION_KEY\)=.*/&/p' "$WORK/global.env" | md5sum)"
check_eq "a second run regenerates nothing (idempotent)" "$SUM_AFTER" "$SUM_BEFORE"
echo

# --- part 5: red control — the b18088e shape is NOT replaced ----------------
echo "part 5 — red control: a concrete value is not recognised (the b18088e shape)"
# Deliberately synthetic: the real published value is not repeated here, not
# even in a test — a credential-shaped string in the tree is the thing this
# file exists to prevent.
CONCRETE="AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMMNNNNOOOOPPPP"
for k in $KEYS_STRING; do
  sed -i "s|^$k=.*|$k=$CONCRETE|" "$WORK/global.env"
done
( cd "$WORK" && bash generate-secrets.sh ) > "$WORK/red.log" 2>&1
check_eq "the concrete value survives the generator" "$(env_value "$WORK/global.env" AIRFLOW_SECRET_KEY)" "$CONCRETE"
check "the log says it was skipped, not generated" \
  "$(grep -q 'AIRFLOW_SECRET_KEY already set — skip' "$WORK/red.log" && echo 0 || echo 1)"
echo

# --- part 6: install.sh's global.env patch writes the host's values in ------
# The other half of the fix: install.sh must put the secrets it just generated
# into global.env BEFORE global-env-sync.py runs, or the sync pushes the
# shipped defaults from global.env over the per-host values in every .env (the
# b18088e failure mode). And it must do that ONLY while global.env still holds
# a shipped default: install.sh promises not to overwrite existing .env files,
# and rotating a live instance's secrets (or handing Dockhand a new at-rest key)
# on a re-run breaks the same promise. Both rules live in install.sh's
# is_shipped_default() / patch_global_secret(), EXTRACTED here, never re-typed,
# so this measures the shipped bytes.
echo "part 6 — install.sh patches the generated secrets into global.env"
{
  sed -n '/^is_shipped_default() {/,/^}/p' "$ROOT/install.sh"
  sed -n '/^patch_global_secret() {/,/^}/p' "$ROOT/install.sh"
} > "$WORK/patch-fns.sh"
check "install.sh still defines is_shipped_default()" \
  "$(grep -q '^is_shipped_default() {' "$WORK/patch-fns.sh" && echo 0 || echo 1)"
check "install.sh still defines patch_global_secret()" \
  "$(grep -q '^patch_global_secret() {' "$WORK/patch-fns.sh" && echo 0 || echo 1)"
check "install.sh still calls patch_global_secret for all five keys" \
  "$(for k in $KEYS_STRING $KEYS_SHAPED; do grep -q "^  patch_global_secret $k " "$ROOT/install.sh" || echo miss; done | grep -q miss && echo 1 || echo 0)"

P_SHARED="SYNTHETIC-shared-secret-0001"
P_FERNET="U1lOVEhFVElDLWZlcm5ldC1rZXktMzItYnl0ZXMtMDE="
P_ENC="U1lOVEhFVElDLWRvY2toYW5kLWtleS0zMi1ieXRlcy0y"
# install.sh's logger, stubbed: the extracted functions log what they did
log() { :; }
# the patch writes `global.env` in its cwd, so give it one
mkdir -p "$WORK/patchdir"
cp "$GLOBAL" "$WORK/patchdir/global.env"
(
  cd "$WORK/patchdir" || exit 1
  # shellcheck disable=SC1090
  . "$WORK/patch-fns.sh"
  patch_global_secret AIRFLOW_SECRET_KEY  "$P_SHARED"
  patch_global_secret AIRFLOW_JWT_SECRET  "$P_SHARED"
  patch_global_secret SUPERSET_SECRET_KEY "$P_SHARED"
  patch_global_secret AIRFLOW_FERNET_KEY  "$P_FERNET"
  patch_global_secret ENCRYPTION_KEY      "$P_ENC"
) > "$WORK/patch.log" 2>&1
check "the extracted patch functions run" $?
for k in $KEYS_STRING; do
  check_eq_secret "install.sh writes $k into global.env" \
    "$(env_value "$WORK/patchdir/global.env" "$k")" "$P_SHARED"
done
check_eq_secret "install.sh writes AIRFLOW_FERNET_KEY into global.env" \
  "$(env_value "$WORK/patchdir/global.env" AIRFLOW_FERNET_KEY)" "$P_FERNET"
check_eq_secret "install.sh writes ENCRYPTION_KEY into global.env" \
  "$(env_value "$WORK/patchdir/global.env" ENCRYPTION_KEY)" "$P_ENC"
check "no sentinel left after the patch" \
  "$(grep -q "$SENTINEL" "$WORK/patchdir/global.env" && echo 1 || echo 0)"
check "the shipped-default comments survive the patch" \
  "$(grep -qc '^# Shipped default' "$WORK/patchdir/global.env" && echo 0 || echo 1)"
# the patch's footprint must be exactly the five secrets — nothing else may move
cp "$GLOBAL" "$WORK/before.rest"
cp "$WORK/patchdir/global.env" "$WORK/after.rest"
CHANGED="$(diff "$WORK/before.rest" "$WORK/after.rest" \
  | sed -n 's/^[<>] \([A-Za-z_][A-Za-z_0-9]*\)=.*/\1/p' | sort -u | tr '\n' ' ')"
check_eq "the patch changes only the five secrets" \
  "$CHANGED" "AIRFLOW_FERNET_KEY AIRFLOW_JWT_SECRET AIRFLOW_SECRET_KEY ENCRYPTION_KEY SUPERSET_SECRET_KEY "
# the re-run promise: a value this host already owns must survive untouched
(
  cd "$WORK/patchdir" || exit 1
  # shellcheck disable=SC1090
  . "$WORK/patch-fns.sh"
  patch_global_secret AIRFLOW_SECRET_KEY  "ANOTHER-shared-secret-9999"
  patch_global_secret AIRFLOW_JWT_SECRET  "ANOTHER-shared-secret-9999"
  patch_global_secret SUPERSET_SECRET_KEY "ANOTHER-shared-secret-9999"
  patch_global_secret AIRFLOW_FERNET_KEY  "U1lOVEhFVElDLWZlcm5ldC1rZXktMzItYnl0ZXMtOTk="
  patch_global_secret ENCRYPTION_KEY      "U1lOVEhFVElDLWRvY2toYW5kLWtleS0zMi1ieXRlcy05OQ=="
) > /dev/null 2>&1
for k in $KEYS_STRING; do
  check_eq_secret "a re-run keeps the per-host $k" \
    "$(env_value "$WORK/patchdir/global.env" "$k")" "$P_SHARED"
done
check_eq_secret "a re-run keeps the per-host AIRFLOW_FERNET_KEY" \
  "$(env_value "$WORK/patchdir/global.env" AIRFLOW_FERNET_KEY)" "$P_FERNET"
check_eq_secret "a re-run keeps the per-host ENCRYPTION_KEY" \
  "$(env_value "$WORK/patchdir/global.env" ENCRYPTION_KEY)" "$P_ENC"
echo

if [ "$FAILS" = 0 ]; then
  echo "SECRET-DEFAULT TESTS: PASS ($CHECKS assertions)"
  exit 0
else
  echo "SECRET-DEFAULT TESTS: FAIL ($FAILS of $CHECKS assertions)"
  exit 1
fi
