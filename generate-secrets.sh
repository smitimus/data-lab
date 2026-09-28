#!/usr/bin/env bash
# =============================================================================
# generate-secrets.sh — standalone idempotent secret generator
#
# Extracted from install.sh so existing clones can generate missing secrets
# without re-running a full install. Safe to run repeatedly — skips secrets
# already present in global.env with non-placeholder values.
#
# What it does:
#   1. Detects environment (IP, timezone, install/conf dirs)
#   2. Generates 3 secret types: Fernet key, shared session secret,
#      Dockhand encryption key
#   3. Checks global.env for existing non-placeholder values before generating
#   4. Writes new secrets to global.env via set_env_var (upsert)
#   5. Runs global-env-sync.py to propagate secrets to all 9 service .env files
#
# Secrets managed:
#   AIRFLOW_FERNET_KEY      – Airflow connection encryption
#   AIRFLOW_SECRET_KEY      – Airflow webserver session signing (shared)
#   AIRFLOW_JWT_SECRET      – Airflow API JWT signing (shared)
#   SUPERSET_SECRET_KEY     – Superset session signing (shared)
#   ENCRYPTION_KEY          – Dockhand credential encryption at rest
#
# Key design:
#   - Secrets go into global.env (not directly into service .env files), so
#     global-env-sync.py owns propagation and won't strip them on next sync.
#   - Shared secret is generated once and reused for AIRFLOW_SECRET_KEY,
#     AIRFLOW_JWT_SECRET, and SUPERSET_SECRET_KEY — matching install.sh.
#   - Placeholder detection: empty, GENERATE_ME_*, and YOUR_* values
#     are treated as "missing". global.env ships its three string secrets as
#     the GENERATE_ME_SECRET sentinel for exactly this reason: the shipped
#     default has to be recognisable, or generation is skipped and the
#     published value survives on every host seeded from the tree
#     (data-lab t_35b04ad9).
#   - The two key-shaped defaults in global.env (AIRFLOW_FERNET_KEY,
#     ENCRYPTION_KEY) cannot be a sentinel — both consumers refuse to start on
#     a malformed key — so they ship as valid, throwaway keys. They are
#     therefore never "missing" and never auto-generated: rotating each one
#     destroys something (stored connection passwords, Dockhand's stored
#     credentials). They are recognised by their plaintext and WARNED about
#     instead — see is_shipped_default_key().
#   - SUPERSET_SECRET_KEY is a sentinel and so IS generated, but it destroys
#     something too: Superset encrypts its stored connection passwords with it,
#     so on an instance that has already provisioned connections the generated
#     value leaves that ciphertext unreadable. The generation branch warns with
#     the re-encrypt command (data-lab t_19e41e00) — the third at-rest key.
# =============================================================================

set -euo pipefail

# --- Paths ------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLOBAL_ENV="${SCRIPT_DIR}/global.env"
SYNC_SCRIPT="${SCRIPT_DIR}/global-env-sync.py"

# --- Colour helpers (when stdout is a terminal) -----------------------------
if [[ -t 1 ]]; then
    bold=$(tput bold); green=$(tput setaf 2); yellow=$(tput setaf 3)
    red=$(tput setaf 1); reset=$(tput sgr0)
else
    bold=; green=; yellow=; red=; reset=
fi

log()     { echo "${bold}[generate-secrets]${reset} $*"; }
success() { echo "  ${green}✓${reset} $*"; }
warn()    { echo "  ${yellow}⚠${reset} $*" >&2; }
err()     { echo "  ${red}✗${reset} $*" >&2; }
info()    { echo "         $*"; }

# --- Secret generators (extracted from install.sh) ---------------------------

# Fernet key: URL-safe base64-encoded 32 random bytes.
# Identical to cryptography.fernet.Fernet.generate_key() but uses only stdlib.
generate_fernet_key() {
    python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
}

# Shared secret: 42 bytes of random, base64-encoded.
# Used by Airflow webserver and Superset for session signing.
generate_secret() {
    openssl rand -base64 42 | tr -d '\n'
}

# Dockhand encryption key: exactly 32 bytes before base64 encoding.
generate_encryption_key() {
    openssl rand -base64 32 | tr -d '\n'
}

# --- Env file helpers --------------------------------------------------------

# set_env_var FILE KEY VALUE
# Upserts a KEY=VALUE line in a .env-style file. If KEY already exists,
# replaces its value; otherwise appends a new line.
set_env_var() {
    local file="$1" key="$2" value="$3"
    if grep -q "^${key}=" "$file" 2>/dev/null; then
        # macOS sed needs '' after -i; Linux doesn't.
        if [[ "$(uname -s)" == "Darwin" ]]; then
            sed -i '' "s|^${key}=.*|${key}=${value}|" "$file"
        else
            sed -i "s|^${key}=.*|${key}=${value}|" "$file"
        fi
    else
        echo "${key}=${value}" >> "$file"
    fi
}

# is_missing VALUE
# Returns 0 (true) if the value should be treated as "not set": empty, starts
# with GENERATE_ME_, or starts with YOUR_.
#
# The sentinel global.env ships for its three string secrets is
# GENERATE_ME_SECRET, which is in this family by design — one vocabulary for
# "this is not a value yet". Its other user is global.env's IP=YOUR_SERVER_IP,
# which the same run replaces with the detected address. Nothing else needs
# adding here for those three: a shipped default that this function does not
# recognise is the bug (it reads as a real secret, generation is skipped, and
# the published value survives).
is_missing() {
    local val="$1"
    [[ -z "$val" ]] && return 0
    [[ "$val" == GENERATE_ME_* ]] && return 0
    [[ "$val" == YOUR_* ]] && return 0
    return 1
}

# is_shipped_default_key VALUE
# Returns 0 (true) if VALUE is one of the two key-shaped shipped defaults that
# global.env carries (AIRFLOW_FERNET_KEY, ENCRYPTION_KEY).
#
# These two are real, valid keys, so is_missing() must NOT call them missing:
# Airflow and Dockhand both refuse to start on a malformed key, which is why
# they cannot be a sentinel like the three string secrets. They are recognised
# by their plaintext instead — both decode to "data-lab-shipped-default-key-00N"
# — so this function stays true to that one convention rather than keeping a
# second copy of the values here.
is_shipped_default_key() {
    local val="$1" decoded
    [[ -z "$val" ]] && return 1
    # urlsafe base64 → standard base64, then decode. Garbage fails the decode.
    decoded=$(printf '%s' "$val" | tr '_-' '/+' | base64 -d 2>/dev/null) || return 1
    [[ "$decoded" == data-lab-shipped-default-key-* ]] && return 0
    return 1
}

# get_env FILE KEY
# Reads the current value of KEY from FILE. Returns empty string if not found.
get_env() {
    local file="$1" key="$2"
    # grep returns 1 on no match — swallow it so set -e doesn't abort
    grep "^${key}=" "$file" 2>/dev/null | head -1 | cut -d= -f2- || true
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "Generate Secrets — idempotent secret generator for data-lab clones"

# --- Pre-flight checks ------------------------------------------------------
if [[ ! -f "$GLOBAL_ENV" ]]; then
    err "global.env not found at $GLOBAL_ENV"
    exit 1
fi

if ! command -v openssl &>/dev/null; then
    err "openssl is required but not installed"
    exit 1
fi

if ! command -v python3 &>/dev/null; then
    err "python3 is required but not installed"
    exit 1
fi

# --- Detect environment -----------------------------------------------------
IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}') || IP="127.0.0.1"
TZ_VAL=$(cat /etc/timezone 2>/dev/null | head -1) || TZ_VAL="America/New_York"
CONF_DIR="${SCRIPT_DIR}/_conf"

info "Detected IP: ${IP}"
info "Detected TZ: ${TZ_VAL}"
info "Install dir: ${SCRIPT_DIR}"
info "Conf dir:    ${CONF_DIR}"

# --- Ensure base env vars are set in global.env -----------------------------
log "Checking base environment variables..."

if is_missing "$(get_env "$GLOBAL_ENV" "IP")"; then
    set_env_var "$GLOBAL_ENV" "IP" "$IP"
    success "Set IP=${IP}"
else
    info "IP already set — skip"
fi

if is_missing "$(get_env "$GLOBAL_ENV" "TZ")"; then
    set_env_var "$GLOBAL_ENV" "TZ" "$TZ_VAL"
    success "Set TZ=${TZ_VAL}"
else
    info "TZ already set — skip"
fi

if is_missing "$(get_env "$GLOBAL_ENV" "STACKS")"; then
    set_env_var "$GLOBAL_ENV" "STACKS" "$SCRIPT_DIR"
    success "Set STACKS=${SCRIPT_DIR}"
else
    info "STACKS already set — skip"
fi

if is_missing "$(get_env "$GLOBAL_ENV" "CONF")"; then
    set_env_var "$GLOBAL_ENV" "CONF" "$CONF_DIR"
    success "Set CONF=${CONF_DIR}"
else
    info "CONF already set — skip"
fi

# --- Generate secrets (only if missing) -------------------------------------
log "Checking secrets..."
GENERATED=0

# Shared secret: used for AIRFLOW_SECRET_KEY, AIRFLOW_JWT_SECRET, SUPERSET_SECRET_KEY
SHARED_EXISTING=$(get_env "$GLOBAL_ENV" "AIRFLOW_SECRET_KEY")
AIRFLOW_JWT_EXISTING=$(get_env "$GLOBAL_ENV" "AIRFLOW_JWT_SECRET")
SUPERSET_EXISTING=$(get_env "$GLOBAL_ENV" "SUPERSET_SECRET_KEY")

# Determine if we need a new shared secret. Generate one if ANY of the three
# target keys are missing (they should all share the same value).
NEED_SHARED=false
if is_missing "$SHARED_EXISTING" || is_missing "$AIRFLOW_JWT_EXISTING" || is_missing "$SUPERSET_EXISTING"; then
    NEED_SHARED=true
    SHARED_SECRET=$(generate_secret)
    info "Generated new shared secret"
fi

# AIRFLOW_SECRET_KEY
if is_missing "$SHARED_EXISTING"; then
    set_env_var "$GLOBAL_ENV" "AIRFLOW_SECRET_KEY" "$SHARED_SECRET"
    success "Set AIRFLOW_SECRET_KEY"
    GENERATED=$((GENERATED + 1))
else
    info "AIRFLOW_SECRET_KEY already set — skip"
    # Use existing value for the other shared keys
    SHARED_SECRET="$SHARED_EXISTING"
fi

# AIRFLOW_JWT_SECRET (reuses shared secret)
if is_missing "$AIRFLOW_JWT_EXISTING"; then
    set_env_var "$GLOBAL_ENV" "AIRFLOW_JWT_SECRET" "$SHARED_SECRET"
    success "Set AIRFLOW_JWT_SECRET"
    GENERATED=$((GENERATED + 1))
else
    info "AIRFLOW_JWT_SECRET already set — skip"
fi

# SUPERSET_SECRET_KEY (reuses shared secret)
if is_missing "$SUPERSET_EXISTING"; then
    set_env_var "$GLOBAL_ENV" "SUPERSET_SECRET_KEY" "$SHARED_SECRET"
    success "Set SUPERSET_SECRET_KEY"
    GENERATED=$((GENERATED + 1))
    # This one cannot be a silent rotation: Superset encrypts the connection
    # passwords it stores with SECRET_KEY, so the value being replaced here is
    # also the key that ciphertext was written under. A fresh clone has nothing
    # stored and can ignore the warning; an instance that has already
    # provisioned connections cannot, and the failure is quiet — login and
    # /api/v1/dashboard stay 200 while /api/v1/database/ 500s (t_19e41e00).
    warn "SUPERSET_SECRET_KEY also encrypts Superset's stored connection passwords."
    warn "  On an instance that has already provisioned them, re-encrypt under the"
    warn "  new key (keep the old value until this succeeds):"
    warn "    docker exec superset superset re-encrypt-secrets"
    warn "      --previous_secret_key \"<the old SUPERSET_SECRET_KEY>\""
    warn "  A fresh clone has nothing stored yet and can ignore this. See global.env (Secrets)."
else
    info "SUPERSET_SECRET_KEY already set — skip"
fi

# AIRFLOW_FERNET_KEY (unique — not shared)
FERNET_EXISTING=$(get_env "$GLOBAL_ENV" "AIRFLOW_FERNET_KEY")
if is_missing "$FERNET_EXISTING"; then
    FERNET_KEY=$(generate_fernet_key)
    set_env_var "$GLOBAL_ENV" "AIRFLOW_FERNET_KEY" "$FERNET_KEY"
    success "Set AIRFLOW_FERNET_KEY"
    GENERATED=$((GENERATED + 1))
else
    info "AIRFLOW_FERNET_KEY already set — skip"
    if is_shipped_default_key "$FERNET_EXISTING"; then
        warn "AIRFLOW_FERNET_KEY still holds the shipped default. Rotating it is a"
        warn "  deliberate step, not a find/replace: it decrypts connection passwords"
        warn "  stored in the Airflow metadata DB. Export the connections under the old"
        warn "  key, change the key, import them under the new one. See global.env (Secrets)."
    fi
fi

# ENCRYPTION_KEY (Dockhand — unique)
ENC_EXISTING=$(get_env "$GLOBAL_ENV" "ENCRYPTION_KEY")
if is_missing "$ENC_EXISTING"; then
    ENC_KEY=$(generate_encryption_key)
    set_env_var "$GLOBAL_ENV" "ENCRYPTION_KEY" "$ENC_KEY"
    success "Set ENCRYPTION_KEY"
    GENERATED=$((GENERATED + 1))
else
    info "ENCRYPTION_KEY already set — skip"
    if is_shipped_default_key "$ENC_EXISTING"; then
        warn "ENCRYPTION_KEY still holds the shipped default. Rotating it makes"
        warn "  Dockhand's stored credentials unreadable, so write the new value to"
        warn "  global.env AND dockhand/.env: a clone that predates t_35b04ad9 still"
        warn "  marks that line service-specific, and the sync preserves such a line"
        warn "  instead of carrying the change across. See global.env (Secrets)."
    fi
fi

# --- Sync secrets to service .env files -------------------------------------
log "Syncing global env vars to service .env files..."
if [[ -f "$SYNC_SCRIPT" ]]; then
    if python3 "$SYNC_SCRIPT"; then
        success "global-env-sync.py completed"
    else
        warn "global-env-sync.py exited non-zero — secrets are in global.env but may not have propagated"
    fi
else
    warn "global-env-sync.py not found at $SYNC_SCRIPT — secrets written to global.env but not propagated"
fi

# --- Summary ----------------------------------------------------------------
echo ""
if [[ "$GENERATED" -gt 0 ]]; then
    log "${GENERATED} new secret(s) generated and written to global.env"
else
    log "All secrets already present — nothing to generate"
fi
echo ""
