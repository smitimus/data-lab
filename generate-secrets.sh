#!/usr/bin/env bash
# =============================================================
# Data Lab — Standalone Secret Generator
#
# Extracted from install.sh so existing clones can generate
# Airflow and Superset secrets without re-running the full
# Docker install.
#
# What this does:
#   1. Detects the install directory and environment (IP, TZ, …)
#   2. Generates Fernet key, shared session secret, and
#      Dockhand encryption key (if not already present)
#   3. Adds the secrets to global.env (idempotent — won't
#      overwrite existing values)
#   4. Runs global-env-sync.py to push everything to every
#      service's .env file
#
# Safe to run repeatedly — existing secrets are preserved.
# =============================================================
set -euo pipefail

# --- Paths ----------------------------------------------------
# Default to /opt/data-lab; fall back to the script's own
# directory so it works for non-/opt clones too.
INSTALL_DIR="${INSTALL_DIR:-/opt/data-lab}"
if [[ ! -d "$INSTALL_DIR" ]]; then
  INSTALL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
CONF_DIR="${CONF_DIR:-$(dirname "$INSTALL_DIR")/conf}"

GLOBAL_ENV="${INSTALL_DIR}/global.env"
SYNC_SCRIPT="${INSTALL_DIR}/global-env-sync.py"

# --- Colour helpers (same palette as install.sh) -------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'
log()  { echo -e "${GREEN}[secrets]${NC} $*"; }
warn() { echo -e "${YELLOW}[secrets]${NC} $*"; }
err()  { echo -e "${RED}[secrets] ERROR:${NC} $*"; exit 1; }

# --- Secret generators (same as install.sh) ------------------

# Fernet key: used by Airflow to encrypt stored connection
# passwords and variables in its metadata database.
generate_fernet_key() {
  python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
}

# Shared secret: used as both the Airflow webserver secret key
# and the Superset SECRET_KEY (both sign session cookies).
generate_secret() {
  openssl rand -base64 42 | tr -d '\n'
}

# Dockhand encryption key: encrypts stored Docker API
# credentials at rest.  Must be exactly 32 bytes.
generate_encryption_key() {
  openssl rand -base64 32 | tr -d '\n'
}

# --- Helpers --------------------------------------------------

# get_env_var FILE KEY — print the current value of KEY from a
# .env-style file, or an empty string if not present / empty.
get_env_var() {
  local file="$1" key="$2"
  grep -E "^${key}=" "$file" 2>/dev/null \
    | head -1 \
    | sed "s/^${key}=//" \
    | sed 's/[[:space:]]*#.*//' \
    | tr -d '"'"'" \
    || true
}

# set_env_var FILE KEY VALUE — upsert KEY=VALUE into FILE.
# Adds at the end if KEY doesn't exist; replaces existing line
# otherwise.  Preserves the rest of the file.
set_env_var() {
  local file="$1" key="$2" value="$3"
  if grep -qE "^${key}=" "$file" 2>/dev/null; then
    # Replace existing line (safe across Linux / macOS).
    sed -i "s|^${key}=.*|${key}=${value}|" "$file"
    log "  Updated ${key} in $(basename "$file")"
  else
    echo "${key}=${value}" >> "$file"
    log "  Added ${key} to $(basename "$file")"
  fi
}

# is_placeholder VAL — returns 0 (true) if VAL looks like a
# placeholder token rather than a real secret.
is_placeholder() {
  local val="$1"
  [[ -z "$val" ]] && return 0
  [[ "$val" == GENERATE_ME_* ]] && return 0
  [[ "$val" == YOUR_* ]] && return 0
  return 1
}

# --- Environment detection (same logic as install.sh) --------

detect_environment() {
  log "Detecting environment…"
  IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}') || IP="127.0.0.1"
  TZ_VAL=$(cat /etc/timezone 2>/dev/null | head -1) || TZ_VAL="America/New_York"
  log "  IP: ${IP}  Timezone: ${TZ_VAL}  Install dir: ${INSTALL_DIR}  Conf dir: ${CONF_DIR}"
}

# --- Patch static placeholders in global.env -----------------
# Only needed when global.env still has YOUR_* tokens
# (e.g. after a fresh clone that never ran install.sh).

patch_global_env_placeholders() {
  local patched=0
  for token in YOUR_SERVER_IP YOUR_INSTALL_DIR YOUR_CONF_DIR YOUR_TIMEZONE; do
    if grep -q "$token" "$GLOBAL_ENV" 2>/dev/null; then
      patched=1
      break
    fi
  done
  if [[ $patched -eq 0 ]]; then
    return 0   # nothing to do
  fi

  log "Patching placeholder tokens in global.env…"
  sed -i \
    -e "s|YOUR_SERVER_IP|${IP}|g" \
    -e "s|YOUR_INSTALL_DIR|${INSTALL_DIR}|g" \
    -e "s|YOUR_CONF_DIR|${CONF_DIR}|g" \
    -e "s|YOUR_TIMEZONE|${TZ_VAL}|g" \
    "$GLOBAL_ENV"
  log "  Placeholders replaced."
}

# --- Main — generate missing secrets -------------------------

main() {
  echo ""
  echo -e "${BOLD}╔══════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}║        Data Lab — Secret Generator              ║${NC}"
  echo -e "${BOLD}╚══════════════════════════════════════════════════╝${NC}"
  echo ""

  # --- Pre-flight checks -------------------------------------
  [[ -f "$GLOBAL_ENV" ]] || err "global.env not found at ${GLOBAL_ENV} — is this a data-lab clone?"
  [[ -f "$SYNC_SCRIPT" ]] || err "global-env-sync.py not found at ${SYNC_SCRIPT}"
  command -v python3  &>/dev/null || err "python3 is required but not found on PATH."
  command -v openssl  &>/dev/null || err "openssl is required but not found on PATH."

  detect_environment
  patch_global_env_placeholders

  # --- Check existing secrets --------------------------------
  log "Checking existing secrets in global.env…"

  local existing_fernet  existing_secret  existing_jwt  existing_superset  existing_dockhand
  existing_fernet=$(get_env_var "$GLOBAL_ENV" AIRFLOW_FERNET_KEY)
  existing_secret=$(get_env_var "$GLOBAL_ENV" AIRFLOW_SECRET_KEY)
  existing_jwt=$(get_env_var "$GLOBAL_ENV" AIRFLOW_JWT_SECRET)
  existing_superset=$(get_env_var "$GLOBAL_ENV" SUPERSET_SECRET_KEY)
  existing_dockhand=$(get_env_var "$GLOBAL_ENV" ENCRYPTION_KEY)

  local generated=0

  # --- Fernet key --------------------------------------------
  if is_placeholder "$existing_fernet"; then
    local fernet
    fernet=$(generate_fernet_key)
    set_env_var "$GLOBAL_ENV" AIRFLOW_FERNET_KEY "$fernet"
    generated=1
  else
    log "  AIRFLOW_FERNET_KEY — already set, skipping."
  fi

  # --- Shared session secret (used for Airflow + Superset) ---
  # Generate ONE value and use it for all three keys so they're
  # consistent (matching install.sh behaviour).
  local need_shared=0
  is_placeholder "$existing_secret"   && need_shared=1
  is_placeholder "$existing_jwt"      && need_shared=1
  is_placeholder "$existing_superset" && need_shared=1

  if [[ $need_shared -eq 1 ]]; then
    local shared
    shared=$(generate_secret)
    is_placeholder "$existing_secret"   && set_env_var "$GLOBAL_ENV" AIRFLOW_SECRET_KEY  "$shared"
    is_placeholder "$existing_jwt"      && set_env_var "$GLOBAL_ENV" AIRFLOW_JWT_SECRET   "$shared"
    is_placeholder "$existing_superset" && set_env_var "$GLOBAL_ENV" SUPERSET_SECRET_KEY  "$shared"
    generated=1
  else
    log "  AIRFLOW_SECRET_KEY / JWT / SUPERSET_SECRET_KEY — already set, skipping."
  fi

  # --- Dockhand encryption key (service-specific) -------------
  # This is *not* a global var in install.sh, but adding it to
  # global.env makes global-env-sync.py manage it consistently.
  # Users who prefer to keep it service-specific can remove it.
  if is_placeholder "$existing_dockhand"; then
    local dockhand
    dockhand=$(generate_encryption_key)
    set_env_var "$GLOBAL_ENV" ENCRYPTION_KEY "$dockhand"
    generated=1
  else
    log "  ENCRYPTION_KEY — already set, skipping."
  fi

  # --- Sync --------------------------------------------------
  if [[ $generated -eq 1 ]]; then
    log "New secrets written to global.env.  Propagating to services…"
  else
    log "All secrets already present — nothing to generate."
  fi

  if python3 "$SYNC_SCRIPT"; then
    log "global-env-sync.py completed successfully."
  else
    warn "global-env-sync.py exited non-zero — check output above."
    warn "(Your secrets were written to global.env; the sync failure does not undo them.)"
    exit 1
  fi

  echo ""
  log "Done.  Restart affected containers to pick up new secrets:"
  echo "       cd ${INSTALL_DIR} && bash restart.sh"
  echo "  or for just Airflow:"
  echo "       docker compose -f ${INSTALL_DIR}/airflow/compose.yaml restart"
}

main "$@"
