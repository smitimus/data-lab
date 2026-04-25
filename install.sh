#!/usr/bin/env bash
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
#   8.  Set vm.max_map_count=262144 (required for OpenMetadata)
#   9.  Run init.sh (seed conf/ dirs)
#   10. Run start.sh (bring up all stacks)
#   11. Wait for Superset, then auto-import dashboards
#   12. Print service table with URLs and default credentials
# =============================================================

REPO_URL="https://github.com/smitimus/data-lab.git"
INSTALL_DIR="/opt/data-lab"
FALLBACK_DIR="${HOME}/data-lab"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'

log()  { echo -e "${GREEN}[data-lab]${NC} $*"; }
warn() { echo -e "${YELLOW}[data-lab]${NC} $*"; }
err()  { echo -e "${RED}[data-lab] ERROR:${NC} $*"; exit 1; }

banner() {
  echo ""
  echo -e "${BOLD}╔══════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}║              Data Lab Installer                  ║${NC}"
  echo -e "${BOLD}╚══════════════════════════════════════════════════╝${NC}"
  echo ""
  echo "This script will install a full analytics engineering stack:"
  echo "  • Docker Engine (if not already installed)"
  echo "  • Airflow + Meltano + dbt + Superset + OpenMetadata"
  echo "  • CloudBeaver + Homepage + Dockhand + PostgreSQL"
  echo "  • Verisim Grocery (mock data generator)"
  echo ""
  echo "Install directory: ${INSTALL_DIR}"
  echo "Estimated time: 5-15 minutes depending on internet speed"
  echo ""
}

install_docker() {
  log "Installing Docker Engine..."
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl gnupg lsb-release
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL "https://download.docker.com/linux/${ID:-debian}/gpg" \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/${ID:-debian} \
    $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
  systemctl enable docker --now
  usermod -aG docker "${SUDO_USER:-$USER}" || true
  log "Docker installed."
}

generate_fernet_key() {
  python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
}

generate_secret() {
  openssl rand -base64 42 | tr -d '\n'
}

generate_encryption_key() {
  openssl rand -base64 32 | tr -d '\n'
}

fill_env() {
  # Args: example target ip tz install_dir docker_gid fernet_key shared_secret enc_key
  local example="$1" target="$2"
  local ip="$3" tz="$4" install_dir="$5" docker_gid="$6"
  local fernet_key="$7" shared_secret="$8" enc_key="$9"

  sed \
    -e "s|YOUR_SERVER_IP|${ip}|g" \
    -e "s|YOUR_TIMEZONE|${tz}|g" \
    -e "s|YOUR_INSTALL_DIR|${install_dir}|g" \
    -e "s|DETECT_ME_DOCKER_GID|${docker_gid}|g" \
    -e "s|GENERATE_ME_FERNET_KEY|${fernet_key}|g" \
    -e "s|GENERATE_ME_SECRET|${shared_secret}|g" \
    -e "s|GENERATE_ME_ENCRYPTION_KEY|${enc_key}|g" \
    "$example" > "$target"
}

wait_for_superset() {
  log "Waiting for Superset to become healthy..."
  local max=60 i=0
  while ! curl -sf http://localhost:8088/health | grep -q '"status".*"OK"'; do
    sleep 5; i=$((i+5))
    [[ $i -ge $max ]] && { warn "Superset not ready after ${max}s — skipping auto-import. Import dashboards manually."; return 1; }
    echo -n "."
  done
  echo ""
  log "Superset is healthy."
}

import_dashboards() {
  local token
  token=$(curl -s -X POST http://localhost:8088/api/v1/security/login \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin","provider":"db"}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))")

  if [[ -z "$token" ]]; then
    warn "Could not authenticate to Superset — skipping dashboard import."
    return 1
  fi

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
  printf "%-20s %-38s %-20s\n" "OpenMetadata"   "http://${ip}:8585"       "admin / admin"
  printf "%-20s %-38s %-20s\n" "Dockhand"       "http://${ip}:3000"       "admin / admin"
  printf "%-20s %-38s %-20s\n" "Verisim UI"     "http://${ip}:8501"       "no auth"
  printf "%-20s %-38s %-20s\n" "Verisim API"    "http://${ip}:8010/docs"  "no auth"
  printf "%-20s %-38s %-20s\n" "PostgreSQL EDW" "${ip}:5432/edw"          "postgres / postgres"
  printf "%-20s %-38s %-20s\n" "Verisim DB"     "${ip}:5499/grocery"      "verisim / verisim"
  echo ""
  echo "Next steps:"
  echo "  1. Trigger the first Airflow DAG: Airflow UI → DAGs → grocery_pipeline → ▶"
  echo "  2. After the DAG completes, check Superset dashboards for data"
  echo "  3. Run setup.sh to adopt all stacks in Dockhand:"
  echo "     cd ${INSTALL_DIR} && bash setup.sh"
  echo ""
}

main() {
  # When run as curl | bash, stdin is the pipe not the terminal.
  # Reconnect to /dev/tty so interactive prompts work. Skip silently
  # if no tty is available (headless/container environments).
  [[ -t 0 ]] || { [[ -c /dev/tty ]] && exec </dev/tty; }

  banner
  echo -e "${BOLD}Press ENTER to continue or Ctrl+C to cancel...${NC}"
  read -r

  # OS check
  if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    if [[ "$ID" != "debian" && "$ID" != "ubuntu" ]]; then
      warn "This installer targets Debian/Ubuntu. Detected: $ID"
      echo "Continue anyway? (y/N)"; read -r ans
      [[ "$ans" =~ ^[Yy]$ ]] || exit 1
    fi
  fi

  # Docker
  if ! command -v docker &>/dev/null; then
    [[ "$EUID" -ne 0 ]] && err "Docker not found. Run with sudo to install Docker."
    install_docker
  else
    log "Docker found: $(docker --version)"
  fi

  # Clone repo
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

  cd "$INSTALL_DIR"

  # Detect environment
  IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}') || IP="127.0.0.1"
  TZ_VAL=$(cat /etc/timezone 2>/dev/null | head -1) || TZ_VAL="America/New_York"
  DOCKER_GID=$(getent group docker | cut -d: -f3) || DOCKER_GID="999"
  log "Detected IP: ${IP}, Timezone: ${TZ_VAL}, Docker GID: ${DOCKER_GID}"

  # Generate secrets — one Fernet key, one shared secret (Airflow/Superset), one Dockhand key
  log "Generating secrets..."
  FERNET_KEY=$(generate_fernet_key)
  SHARED_SECRET=$(generate_secret)
  DOCKHAND_KEY=$(generate_encryption_key)

  # Generate .env files from .env.example
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
      "$FERNET_KEY" "$SHARED_SECRET" "$DOCKHAND_KEY"
    log "Generated ${target}"
  done < <(find . -name '.env.example' -print0)

  # Also update global.env
  sed -i \
    -e "s|YOUR_SERVER_IP|${IP}|g" \
    -e "s|YOUR_INSTALL_DIR|${INSTALL_DIR}|g" \
    -e "s|YOUR_TIMEZONE|${TZ_VAL}|g" \
    global.env

  # Sync globals to all .env files
  log "Syncing global env vars..."
  python3 global-env-sync.py

  # OpenMetadata sysctl requirement
  log "Setting vm.max_map_count=262144 (required for OpenMetadata/OpenSearch)..."
  sysctl -w vm.max_map_count=262144
  if ! grep -q "vm.max_map_count" /etc/sysctl.conf; then
    echo "vm.max_map_count=262144" >> /etc/sysctl.conf
  fi

  # Init + start
  log "Seeding conf/ directories..."
  bash init.sh

  log "Starting all stacks..."
  bash start.sh

  # Dashboard import
  if wait_for_superset; then
    import_dashboards
  fi

  print_services "$IP"
}

main "$@"
