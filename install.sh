#!/usr/bin/env bash
# Exit immediately on error (-e), treat unset variables as errors (-u),
# and propagate pipe failures so a failed left-hand command isn't silently swallowed (-o pipefail).
set -euo pipefail

# =============================================================
# Data Lab — One-Liner Installer
# What this does:
#   1.  Check OS (warn if not Debian/Ubuntu)
#   2.  Install Docker Engine if not present
#   3.  Clone smitimus/data-lab to /opt/data-lab
#   4.  Auto-detect server IP and Docker GID
#   5.  Generate secrets (Fernet, JWT, Superset key, Dockhand key)
#   6.  Generate all .env files from .env.example templates
#   7.  Run global-env-sync.py to propagate globals
#   8.  Run init.sh (seed conf/ dirs)
#   9.  Run start.sh (bring up all stacks)
#   10. Wait for Dockhand, then auto-adopt all stacks
#   11. Launch background job: wait for Superset, then auto-import dashboards
#   12. Print service table with URLs and default credentials
# =============================================================

# GitHub repo to clone and the preferred install location.
# FALLBACK_DIR is used if /opt is not writable by the current user.
REPO_URL="https://github.com/smitimus/data-lab.git"
INSTALL_DIR="/opt/data-lab"
FALLBACK_DIR="${HOME}/data-lab"

# AUTO_YES=true skips all interactive prompts (set via -y / --yes flag).
# Used for automated testing; equivalent to pressing Enter/Y at every prompt.
AUTO_YES=false
for arg in "$@"; do
  [[ "$arg" == "-y" || "$arg" == "--yes" ]] && AUTO_YES=true
done

# ANSI color codes for terminal output. NC = No Color (reset).
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'

# Logging helpers: log (green info), warn (yellow warning), err (red fatal).
log()  { echo -e "${GREEN}[data-lab]${NC} $*"; }
warn() { echo -e "${YELLOW}[data-lab]${NC} $*"; }
err()  { echo -e "${RED}[data-lab] ERROR:${NC} $*"; exit 1; }

# ------------------------------------------------------------
# banner — prints the welcome screen shown at script start.
# ------------------------------------------------------------
banner() {
  echo ""
  echo -e "${BOLD}╔══════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}║              Data Lab Installer                  ║${NC}"
  echo -e "${BOLD}╚══════════════════════════════════════════════════╝${NC}"
  echo ""
  echo "This script will install a full analytics engineering stack:"
  echo "  • Docker Engine (if not already installed)"
  echo "  • Airflow + Meltano + dbt + Superset + dbt Docs"
  echo "  • CloudBeaver + Homepage + Dockhand + PostgreSQL"
  echo "  • Verisim Grocery (mock data generator)"
  echo ""
  echo "Install directory: ${INSTALL_DIR}"
  echo "Estimated time: 5-15 minutes depending on internet speed"
  echo ""
}

# ------------------------------------------------------------
# install_docker — full Docker Engine install for Debian/Ubuntu.
# Uses the official Docker apt repository rather than the distro
# package, which may be outdated.  Requires root.
# ------------------------------------------------------------
install_docker() {
  log "Installing Docker Engine..."

  # Install the packages needed to add a signed apt repository over HTTPS.
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl gnupg lsb-release

  # Create the directory that holds trusted apt signing keys, then import
  # Docker's GPG key so apt can verify downloaded packages are genuine.
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL "https://download.docker.com/linux/${ID:-debian}/gpg" \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg

  # Add the Docker stable channel to apt's sources.  ${ID} comes from
  # sourcing /etc/os-release earlier (debian or ubuntu).  $(lsb_release -cs)
  # returns the distro codename (e.g. bookworm, jammy).
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/${ID:-debian} \
    $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq

  # Install the Docker Engine, CLI client, containerd runtime, and the
  # Compose plugin (enables `docker compose` sub-command).
  apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin

  # Enable the Docker daemon to start on boot and start it right now.
  systemctl enable docker --now

  # Add the calling user to the docker group so they can run docker commands
  # without sudo.  SUDO_USER is set when the script was invoked via sudo;
  # if not set, fall back to $USER.  The `|| true` prevents a failure if the
  # user is already in the group.
  usermod -aG docker "${SUDO_USER:-$USER}" || true
  log "Docker installed."
}

# ------------------------------------------------------------
# Secret generators — each produces a single-line random value.
# ------------------------------------------------------------

# Fernet key: used by Airflow to encrypt connection passwords and variables
# stored in its database.  Must be URL-safe base64-encoded 32 random bytes —
# identical to what cryptography.fernet.Fernet.generate_key() produces, but
# uses only Python stdlib so no pip install is required.
generate_fernet_key() {
  python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
}

# Shared secret: used as both the Airflow webserver secret key and the
# Superset SECRET_KEY, both of which sign session cookies and CSRF tokens.
generate_secret() {
  openssl rand -base64 42 | tr -d '\n'
}

# Dockhand encryption key: Dockhand uses this to encrypt stored Docker API
# credentials at rest.  Must be exactly 32 bytes before base64 encoding.
generate_encryption_key() {
  openssl rand -base64 32 | tr -d '\n'
}

# ------------------------------------------------------------
# fill_env EXAMPLE TARGET IP TZ INSTALL_DIR DOCKER_GID
#           FERNET_KEY SHARED_SECRET ENC_KEY CONF_DIR
#
# Generates a .env file from a .env.example template by substituting
# every placeholder token with its real value using sed.
#
# Placeholder tokens used in templates:
#   YOUR_SERVER_IP          → detected LAN IP of this machine
#   YOUR_TIMEZONE           → system timezone (e.g. America/New_York)
#   YOUR_INSTALL_DIR        → path where the repo was cloned
#   YOUR_CONF_DIR           → path where runtime config/data is stored
#   DETECT_ME_DOCKER_GID    → numeric GID of the docker group
#   GENERATE_ME_FERNET_KEY  → Airflow Fernet key
#   GENERATE_ME_SECRET      → shared session signing key
#   GENERATE_ME_ENCRYPTION_KEY → Dockhand encryption key
# ------------------------------------------------------------
fill_env() {
  local example="$1" target="$2"
  local ip="$3" tz="$4" install_dir="$5" docker_gid="$6"
  local fernet_key="$7" shared_secret="$8" enc_key="$9" conf_dir="${10}"

  sed \
    -e "s|YOUR_SERVER_IP|${ip}|g" \
    -e "s|YOUR_TIMEZONE|${tz}|g" \
    -e "s|YOUR_INSTALL_DIR|${install_dir}|g" \
    -e "s|YOUR_CONF_DIR|${conf_dir}|g" \
    -e "s|DETECT_ME_DOCKER_GID|${docker_gid}|g" \
    -e "s|GENERATE_ME_FERNET_KEY|${fernet_key}|g" \
    -e "s|GENERATE_ME_SECRET|${shared_secret}|g" \
    -e "s|GENERATE_ME_ENCRYPTION_KEY|${enc_key}|g" \
    "$example" > "$target"
}

# ------------------------------------------------------------
# adopt_stacks — waits for Dockhand then bulk-adopts all stacks via adopt.py.
# Non-fatal: prints a warning and continues if Dockhand doesn't come up.
# ------------------------------------------------------------
adopt_stacks() {
  log "Waiting for Dockhand to become ready..."
  local max=60 i=0
  until curl -sf http://localhost:3000 -o /dev/null 2>/dev/null; do
    sleep 3; i=$((i+3))
    if [[ $i -ge $max ]]; then
      warn "Dockhand not ready after ${max}s — run 'bash setup.sh' manually to adopt stacks."
      return 0
    fi
  done
  log "Dockhand is up — adopting stacks..."
  STACKS="${INSTALL_DIR}" python3 "${INSTALL_DIR}/dockhand/adopt.py" \
    --username admin --password admin \
    && log "All stacks adopted into Dockhand." \
    || warn "Stack adoption failed — run 'bash setup.sh' manually."
}

# ------------------------------------------------------------
# wait_for_superset — polls Superset's /health endpoint (returns plain "OK")
# every 10 seconds until HTTP 200 is returned.  superset init typically takes
# 10-15 minutes on first run; this is called from a background job so
# install.sh can exit immediately and not block the user.
# Times out after 1800 seconds (30 min) and returns non-zero on failure.
# ------------------------------------------------------------
wait_for_superset() {
  log "Waiting for Superset to become healthy (superset init takes ~10-15 min on first run)..."
  local max=1800 i=0
  while ! curl -sf http://localhost:8088/health -o /dev/null 2>/dev/null; do
    sleep 10; i=$((i+10))
    [[ $i -ge $max ]] && { warn "Superset not ready after ${max}s — import dashboards manually."; return 1; }
    [[ $((i % 60)) -eq 0 ]] && log "Still waiting for Superset... (${i}s elapsed)"
  done
  log "Superset is healthy."
}

# ------------------------------------------------------------
# import_dashboards — authenticates to Superset's REST API and imports
# every .zip file found in the repo's superset/dashboards/ directory.
# Dashboard zips are pre-exported from a configured Superset instance
# and bundled with the repo for automatic provisioning.
# ------------------------------------------------------------
import_dashboards() {
  # POST admin credentials to obtain a short-lived JWT access token.
  local token
  token=$(curl -s -X POST http://localhost:8088/api/v1/security/login \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin","provider":"db"}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))")

  if [[ -z "$token" ]]; then
    warn "Could not authenticate to Superset — skipping dashboard import."
    return 1
  fi

  # Import each zip.  The `passwords` field supplies the database password
  # Superset needs to recreate the EDW connection definition bundled in the zip.
  for zip in "${INSTALL_DIR}"/superset/dashboards/*.zip; do
    [[ -f "$zip" ]] || continue
    log "Importing dashboard: $(basename "$zip")"
    curl -s -X POST http://localhost:8088/api/v1/dashboard/import/ \
      -H "Authorization: Bearer $token" \
      -F "formData=@${zip}" \
      -F 'passwords={"databases/EDW.yaml":"postgres"}' \
      > /dev/null && log "Imported $(basename "$zip")" || warn "Failed to import $(basename "$zip")"
  done
}

# ------------------------------------------------------------
# print_services IP — prints the final service table with URLs and
# default credentials once the stack is fully up.
# ------------------------------------------------------------
print_services() {
  local ip="$1"
  echo ""
  echo -e "${BOLD}${GREEN}Data Lab is running!${NC}"
  echo ""
  printf "%-20s %-38s %-20s\n" "SERVICE" "URL" "CREDENTIALS"
  printf "%-20s %-38s %-20s\n" "-------" "---" "-----------"
  printf "%-20s %-38s %-20s\n" "Homepage"       "http://${ip}:80"         "no auth"
  printf "%-20s %-38s %-20s\n" "Airflow"        "http://${ip}:8080"       "admin / admin"
  printf "%-20s %-38s %-20s\n" "Superset"       "http://${ip}:8088"       "admin / admin"
  printf "%-20s %-38s %-20s\n" "CloudBeaver"    "http://${ip}:8978"       "set on first login"
  printf "%-20s %-38s %-20s\n" "dbt Docs"       "http://${ip}:8082"       "no auth"
  printf "%-20s %-38s %-20s\n" "Dockhand"       "http://${ip}:3000"       "admin / admin"
  printf "%-20s %-38s %-20s\n" "Verisim UI"     "http://${ip}:8501"       "no auth"
  printf "%-20s %-38s %-20s\n" "Verisim API"    "http://${ip}:8010/docs"  "no auth"
  printf "%-20s %-38s %-20s\n" "PostgreSQL EDW" "${ip}:5432/edw"          "postgres / postgres"
  printf "%-20s %-38s %-20s\n" "Verisim DB"     "${ip}:5499/grocery"      "verisim / verisim"
  echo ""
  echo "Next steps:"
  echo "  1. Trigger the first Airflow DAG: Airflow UI → DAGs → grocery_pipeline → ▶"
  echo "  2. After the DAG completes, check Superset dashboards for data"
  echo ""
}

# ============================================================
# main — orchestrates the full install sequence.
# ============================================================
main() {
  # --- TTY reconnect -------------------------------------------------------
  # When invoked as `curl ... | bash`, stdin is the pipe, not the terminal.
  # Try to reconnect stdin to /dev/tty so interactive prompts work.
  # The `|| true` prevents set -e from aborting if /dev/tty is inaccessible
  # (e.g. SSH session without PTY allocation, or a container with no tty).
  [[ -t 0 ]] || exec </dev/tty 2>/dev/null || true

  banner
  if [[ "$AUTO_YES" == "true" ]]; then
    log "Running in non-interactive mode (-y)."
  else
    echo -e "${BOLD}Press ENTER to continue or Ctrl+C to cancel...${NC}"
    read -r
  fi

  # --- OS check ------------------------------------------------------------
  # Source /etc/os-release to populate distro variables ($ID, $VERSION_ID,
  # etc.).  Only Debian and Ubuntu are officially supported; other distros
  # may work but have not been tested and are not guaranteed.
  if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    if [[ "$ID" != "debian" && "$ID" != "ubuntu" ]]; then
      warn "This installer targets Debian/Ubuntu. Detected: $ID"
      if [[ "$AUTO_YES" == "true" ]]; then
        warn "Non-interactive mode: continuing anyway."
      else
        echo "Continue anyway? (y/N)"; read -r ans
        [[ "$ans" =~ ^[Yy]$ ]] || exit 1
      fi
    fi
  fi

  # --- Docker check --------------------------------------------------------
  # If `docker` is not on PATH, attempt to install it via install_docker().
  # That function requires root, so we bail early with a helpful message if
  # the script is not running as root.
  if ! command -v docker &>/dev/null; then
    [[ "$EUID" -ne 0 ]] && err "Docker not found. Run with sudo to install Docker."
    install_docker
  else
    log "Docker found: $(docker --version)"
  fi

  # --- Clone repo ----------------------------------------------------------
  # Clone the data-lab repo to INSTALL_DIR (/opt/data-lab).  If /opt is not
  # writable (running without root), fall back to FALLBACK_DIR (~/data-lab).
  # Safe to re-run: the clone is skipped if the directory already contains
  # a git repo (identified by the presence of a .git subdirectory).
  if [[ -d "${INSTALL_DIR}/.git" ]]; then
    log "Repo already cloned at ${INSTALL_DIR} — skipping clone."
  else
    ACTUAL_DIR="${INSTALL_DIR}"
    if [[ ! -w "$(dirname "${INSTALL_DIR}")" ]]; then
      warn "/opt not writable — installing to ${FALLBACK_DIR}"
      ACTUAL_DIR="${FALLBACK_DIR}"
    fi
    log "Cloning data-lab to ${ACTUAL_DIR}..."
    git clone "$REPO_URL" "$ACTUAL_DIR"
    INSTALL_DIR="$ACTUAL_DIR"
  fi

  # Change into the repo directory so all relative paths resolve correctly
  # for the rest of the script.
  cd "$INSTALL_DIR"

  # --- Detect environment --------------------------------------------------
  # IP:         outbound source address — used in service URLs and .env files.
  #             Determined by asking the kernel which local address would be
  #             used to reach 1.1.1.1 (no packet is actually sent).
  # TZ_VAL:     system timezone from /etc/timezone, propagated to containers.
  # DOCKER_GID: numeric GID of the docker group.  Containers that mount
  #             /var/run/docker.sock need to run as this GID to access it.
  # CONF_DIR:   runtime data directory — always a sibling of the repo dir
  #             (e.g. repo at /opt/data-lab → conf at /opt/conf).
  IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}') || IP="127.0.0.1"
  TZ_VAL=$(cat /etc/timezone 2>/dev/null | head -1) || TZ_VAL="America/New_York"
  DOCKER_GID=$(getent group docker | cut -d: -f3) || DOCKER_GID="999"
  CONF_DIR="$(dirname "$INSTALL_DIR")/conf"
  log "Detected IP: ${IP}, Timezone: ${TZ_VAL}, Docker GID: ${DOCKER_GID}"

  # --- Generate secrets ----------------------------------------------------
  # Three distinct secrets are generated fresh for every new install:
  #   FERNET_KEY:    Airflow encrypts stored connection passwords with this.
  #   SHARED_SECRET: Airflow and Superset both use this to sign session cookies.
  #   DOCKHAND_KEY:  Dockhand encrypts stored Docker API credentials with this.
  # They are written into .env files and never leave this machine.
  log "Generating secrets..."
  FERNET_KEY=$(generate_fernet_key)
  SHARED_SECRET=$(generate_secret)
  DOCKHAND_KEY=$(generate_encryption_key)

  # --- Generate .env files -------------------------------------------------
  # Every service subdirectory contains a .env.example template with
  # placeholder tokens.  fill_env substitutes all placeholders and writes
  # the final .env alongside the compose.yaml.
  # Existing .env files are never overwritten — safe to re-run after a
  # partial or failed install.
  log "Generating .env files..."
  while IFS= read -r -d '' example; do
    dir=$(dirname "$example")
    target="${dir}/.env"
    if [[ -f "$target" ]]; then
      log "Skipping ${target} (already exists)"
      continue
    fi
    fill_env "$example" "$target" \
      "$IP" "$TZ_VAL" "$INSTALL_DIR" "$DOCKER_GID" \
      "$FERNET_KEY" "$SHARED_SECRET" "$DOCKHAND_KEY" "$CONF_DIR"
    log "Generated ${target}"
  done < <(find . -name '.env.example' -print0)

  # --- Patch global.env ----------------------------------------------------
  # global.env stores variables shared across all services and uses the same
  # placeholder tokens as the .env.example files.  It must be filled in before
  # global-env-sync.py runs because that script reads it as its source of truth.
  sed -i \
    -e "s|YOUR_SERVER_IP|${IP}|g" \
    -e "s|YOUR_INSTALL_DIR|${INSTALL_DIR}|g" \
    -e "s|YOUR_CONF_DIR|${CONF_DIR}|g" \
    -e "s|YOUR_TIMEZONE|${TZ_VAL}|g" \
    global.env

  # --- Sync global env vars ------------------------------------------------
  # global-env-sync.py reads global.env and pushes every variable it defines
  # into each service's .env file.  This keeps shared values (IP, TZ, CONF,
  # STACKS, etc.) consistent without duplicating them in every template.
  log "Syncing global env vars..."
  python3 global-env-sync.py

  # --- Seed conf/ and start stacks -----------------------------------------
  # init.sh creates all runtime directories under CONF_DIR and copies seed
  # config files (superset_config.py, meltano.yml, etc.) into place so
  # containers have their initial configuration on first boot.
  log "Seeding conf/ directories..."
  bash init.sh

  # start.sh brings up every Docker Compose stack in the correct dependency
  # order (postgres first, then all dependents).
  log "Starting all stacks..."
  bash start.sh

  # --- Adopt stacks into Dockhand ------------------------------------------
  adopt_stacks

  # --- Auto-import Superset dashboards (background) ------------------------
  # superset init takes 20-40 min on first run, so the wait+import runs in
  # the background. install.sh exits immediately after printing the service
  # table; the import completes on its own and logs to SUPERSET_LOG.
  local SUPERSET_LOG=/tmp/superset-import.log
  (
    if wait_for_superset; then
      import_dashboards
      log "Dashboard import complete."
    fi
  ) >> "$SUPERSET_LOG" 2>&1 &
  log "Superset dashboard import running in background — tail $SUPERSET_LOG to monitor"

  # --- Done ----------------------------------------------------------------
  print_services "$IP"
}

main "$@"
