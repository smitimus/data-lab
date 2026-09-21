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
#   8.  Run init.sh (seed _conf/ dirs)
#   9.  Run start.sh (bring up all stacks)
#   10. Wait for Dockhand, then auto-adopt all stacks
#   11. Launch background job: wait for Superset, then import the bundled
#       dashboards — but only once the marts exist. Before the first `transform`
#       their datasets have no tables behind them and the charts land unable to
#       render, so the import DEFERS there and names the re-run command
#       (`install.sh --dashboards-only`, see the block above main).
#   12. Print service table with URLs and default credentials
# =============================================================

# GitHub repo to clone and the preferred install location.
# FALLBACK_DIR is used if /opt is not writable by the current user.
# INSTALL_DIR can be overridden from the environment — `--dashboards-only` uses
# that (and FALLBACK_DIR) to find the tree it is re-running the import against.
REPO_URL="https://github.com/smitimus/data-lab.git"
INSTALL_DIR="${INSTALL_DIR:-/opt/data-lab}"
FALLBACK_DIR="${HOME}/data-lab"

# AUTO_YES=true skips all interactive prompts (set via -y / --yes flag).
# Used for automated testing; equivalent to pressing Enter/Y at every prompt.
# DASHBOARDS_ONLY=1 (--dashboards-only) runs just the bundled-dashboard import
# against an already-installed host; see the block above main.
AUTO_YES=false
DASHBOARDS_ONLY=0
for arg in "$@"; do
  case "$arg" in
    -y|--yes)          AUTO_YES=true ;;
    --dashboards-only) DASHBOARDS_ONLY=1 ;;
  esac
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
# Superset dashboards — import the bundled zips only once the marts exist,
# then report per object what actually landed. Same contract as
# scripts/app-layer.sh step 8b on the infra host, so both hosts behave alike.
#
# The zips in superset/dashboards/ carry one Superset dataset per mart table.
# Imported before the first `transform` has run — which at install time is
# always the case, the first pipeline run is triggered by hand afterwards —
# those datasets have no table behind them, the charts land unable to render,
# and Superset still answers HTTP 200 {"message": "OK"} for a bundle it only
# partly placed, 422 for one it rejected. The old step threw the response body
# away (`> /dev/null`) and logged "Imported <zip>" for any status curl could
# complete, so a rejected or half-landed bundle looked exactly like a good one.
# Dev, 2026-09-21: 18 charts with query_context null — exactly this bundle's
# charts — which is what the gate fails on (e2e-testing/full-cycle.sh phase 8).
#
# So this block: waits (bounded) for the mart schema and DEFERS loudly, naming
# the re-run command, when it is not there yet; imports idempotently with
# overwrite=true; then reads Superset's own meta DB and reports per object,
# plus the gate's own two assertions (datasource_id, query_context). Deferred
# is not a failure — but an import that ran and left Superset incomplete is,
# and it is the exit status of `--dashboards-only` (DASH_STRICT=0 downgrades it).
# ------------------------------------------------------------
SUP_URL="${SUP_URL:-http://localhost:8088}"
SUP_META_DB="${SUP_META_DB:-superset}"   # Superset's own meta DB, in the postgres container
EDW_DB="${EDW_DB:-grocery}"              # the DB holding the mart schema
DASH_DIR="${DASH_DIR:-}"                 # defaulted against INSTALL_DIR in superset_dashboards()
MART_MIN="${MART_MIN:-42}"               # the gate's bar: 42 mart relations (full-cycle.sh 8b)
DASH_MIN="${DASH_MIN:-11}"               # the gate's bar: 11+ dashboards
DASH_WAIT="${DASH_WAIT:-120}"            # seconds to wait for the first transform
DASH_STRICT="${DASH_STRICT:-1}"          # 1 = an incomplete import fails the re-run

# ------------------------------------------------------------
# psql_q DB SQL -> rows, unaligned, no trailing blanks (empty on any failure).
# Reads Superset's own meta DB and the EDW through the running postgres
# container; every failure is an empty answer, never a set -e abort.
# ------------------------------------------------------------
psql_q() {
  { docker exec postgres psql -U postgres -d "$1" -tAc "$2" 2>/dev/null || true; } | sed 's/[[:space:]]*$//'
}

# ------------------------------------------------------------
# zipquery ZIP MODE(tables|counts|passwords|uuids:KIND) — read the bundle
# itself, no Superset needed: which mart tables it expects, the password map
# the importer wants, and the uuid of every object it carries.
# ------------------------------------------------------------
zipquery() {
  python3 -c '
import json, re, sys, zipfile

path, mode = sys.argv[1], sys.argv[2]
z = zipfile.ZipFile(path)
names = [n for n in z.namelist() if n.endswith(".yaml")]

def field(name, key):
    body = z.read(name).decode("utf-8", "replace")
    m = re.search(r"^" + key + r":\s*(\S+)", body, re.M)
    return m.group(1) if m else None

def label(name):
    # full value, spaces included (slice_name / dashboard_title carry them)
    body = z.read(name).decode("utf-8", "replace")
    for key in ("slice_name", "dashboard_title", "table_name"):
        m = re.search(r"^" + key + r":\s*(.+?)\s*$", body, re.M)
        if m:
            return m.group(1).strip().strip("\"")
    return name.rsplit("/", 1)[1][:-5]

def of(kind):
    return [n for n in names if ("/" + kind + "/") in n]

if mode == "tables":
    for n in of("datasets"):
        print("%s.%s" % (field(n, "schema") or "mart", field(n, "table_name")))
elif mode == "counts":
    print(" ".join("%s=%d" % (k, len(of(k))) for k in ("dashboards", "charts", "datasets", "databases")))
elif mode == "passwords":
    print(json.dumps({n.split("/", 1)[1]: "postgres" for n in of("databases")}))
elif mode.startswith("uuids:"):
    for n in of(mode.split(":", 1)[1]):
        u = field(n, "uuid")
        if u:
            print("%s\t%s" % (u.lower(), label(n)))
' "$1" "$2" 2>/dev/null || true
}

count_matches() { # NEEDLES HAYSTACK -> how many needles appear exactly in haystack
  { printf '%s\n' "$1" | grep -Fxf <(printf '%s\n' "$2") || true; } | grep -c . || true
}
missing_of() {    # NEEDLES HAYSTACK -> the needles absent from haystack
  printf '%s\n' "$1" | grep -Fxv -f <(printf '%s\n' "$2") || true
}

needed_mart_tables() { # every mart table the bundled dashboards need, deduped
  local zip
  for zip in "$DASH_DIR"/*.zip; do
    [[ -f "$zip" ]] || continue
    zipquery "$zip" tables
  done | sort -u
}

# ------------------------------------------------------------
# wait_for_marts — 0 = the first transform has landed, 1 = it has not (and
# says what is missing). `information_schema.tables where table_schema='mart'`
# is the cheap probe: dbt materialises all 42 mart models as tables, so the
# count climbs as the CTAS statements land. Bounded by DASH_WAIT.
# ------------------------------------------------------------
wait_for_marts() {
  local waited=0 n=0 have="" missing="" needed
  needed="$(needed_mart_tables)"
  if [[ -z "$needed" ]]; then
    log "bundled dashboards reference no mart tables"
    return 0
  fi
  while :; do
    have="$(psql_q "$EDW_DB" "select table_name from information_schema.tables where table_schema='mart'")"
    n="$(printf '%s\n' "$have" | grep -c . || true)"
    missing="$(missing_of "$(printf '%s\n' "$needed" | cut -d. -f2)" "$have")"
    if [[ -z "$missing" && "${n:-0}" -ge "$MART_MIN" ]]; then
      log "marts ready: $n tables in mart, every bundled dataset resolves"
      return 0
    fi
    if [[ "$waited" -ge "$DASH_WAIT" ]]; then
      if [[ -n "$missing" ]]; then
        warn "marts still missing after ${waited}s: $(printf '%s' "$missing" | tr '\n' ' ')"
      fi
      warn "mart tables: ${n:-0} (need >= $MART_MIN) — the first transform has not finished"
      return 1
    fi
    if [[ $((waited % 60)) -eq 0 ]]; then
      log "waiting for the first transform: ${n:-0} marts, ${waited}s/${DASH_WAIT}s${missing:+, missing: $(printf '%s' "$missing" | tr '\n' ' ')}"
    fi
    sleep 20; waited=$((waited + 20))
  done
}

# ------------------------------------------------------------
# report_import_body FILE — Superset reports per-object failures in the
# response body only (the HTTP status alone cannot distinguish "imported
# everything" from "dropped half the bundle"), so the body is printed, not
# discarded.
# ------------------------------------------------------------
report_import_body() {
  [[ -s "$1" ]] || { warn "  (empty response body)"; return 0; }
  python3 -c '
import json, sys

raw = open(sys.argv[1]).read().strip()
try:
    doc = json.loads(raw)
except Exception:
    print("    " + raw[:1000].replace("\n", " "))
    raise SystemExit(0)
if isinstance(doc, dict) and len(doc) == 1 and isinstance(doc.get("message"), str):
    print("    " + doc["message"])
else:
    for line in json.dumps(doc, indent=2)[:1500].splitlines():
        print("    " + line)
' "$1" || true
}

# ------------------------------------------------------------
# bundle_charts_zip ZIP OUT — a chart-importable copy of a dashboard bundle.
#
# Superset's dashboard importer calls import_chart(config, overwrite=False)
# (superset/commands/dashboard/importers/v1/__init__.py) — hardcoded — so a chart
# that already exists is returned UNTOUCHED and the bundle's query_context never
# reaches it. That is why an instance whose bundled charts were created before
# the bundle carried a query_context stays broken through any number of
# re-imports, and why the deploy has to refresh the charts itself.
#
# The CHART importer passes the caller's overwrite through, so re-submitting the
# bundle's object set through /api/v1/chart/import/ is what actually updates
# those charts. Two things make the copy acceptable to it: metadata.yaml declares
# `type: Dashboard` and that command validates it against `Slice`, so it is
# rewritten here; and the dashboards/ entries are dropped, because a chart
# import has no schema for them (load_configs skips a prefix it has no schema
# for, but so does the dashboard import re-run, which would be a no-op).
# ------------------------------------------------------------
bundle_charts_zip() { # ZIP OUT
  # stdlib only: the guest is a bare Debian host (no PyYAML), which is also why
  # zipquery above parses the bundle with re, not yaml. Only one line of
  # metadata.yaml changes, so it is rewritten in place.
  python3 - "$1" "$2" <<'PY' || return 1
import re
import sys
import zipfile

src, dst = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(src) as zin:
    keep = [(i, zin.read(i.filename)) for i in zin.infolist()
            if "/dashboards/" not in i.filename]
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zout:
    for info, data in keep:
        if info.filename.endswith("metadata.yaml"):
            # what ImportChartsCommand validates against, in place of Dashboard
            data, n = re.subn(r"(?m)^type:\s*\S+", "type: Slice", data.decode())
            if n != 1:
                raise SystemExit("metadata.yaml has no single `type:` line (found %d)" % n)
            data = data.encode()
        zout.writestr(info.filename, data)
PY
}

# ------------------------------------------------------------
# import_dashboards — 0 = every zip imported; 1 = at least one did not (detail
# printed). Captures the HTTP status AND the response body of every import.
# ------------------------------------------------------------
import_dashboards() {
  local tok zip pw_json code body rc=0 charts_zip ccode
  tok="$(curl -s --max-time 15 -X POST "$SUP_URL/api/v1/security/login" \
      -H 'Content-Type: application/json' \
      -d '{"username":"admin","password":"admin","provider":"db"}' \
      | python3 -c 'import json,sys;print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null || true)"
  if [[ -z "$tok" ]]; then
    warn "could not authenticate to Superset (admin/admin) — dashboards NOT imported"
    warn "  re-run once Superset is healthy: bash ${INSTALL_DIR}/install.sh --dashboards-only"
    return 1
  fi
  for zip in "$DASH_DIR"/*.zip; do
    [[ -f "$zip" ]] || continue
    pw_json="$(zipquery "$zip" passwords || echo '{}')"
    [[ -n "$pw_json" ]] || pw_json='{}'
    body="$(mktemp)"
    # overwrite=true is what makes a re-run work at all: Superset 4.1.2 rejects
    # the whole bundle with 422 ("already exists and `overwrite=true` was not
    # passed") when a bundled dashboard uuid is already present, and that
    # rejection is atomic. Note that it only governs the DASHBOARDS: existing
    # charts and datasets are returned untouched, which is why the chart refresh
    # below is a separate call.
    code="$(curl -s --max-time 300 -o "$body" -w '%{http_code}' \
        -X POST "$SUP_URL/api/v1/dashboard/import/" \
        -H "Authorization: Bearer ${tok}" -H 'Accept: application/json' \
        -F "formData=@$zip" -F "passwords=$pw_json" -F 'overwrite=true' 2>/dev/null || echo 000)"
    if [[ "$code" == "200" ]]; then
      log "imported $(basename "$zip") (HTTP 200, overwrite=true)"
      # ...but the dashboard import never overwrites an existing CHART (see
      # bundle_charts_zip), so the bundle's charts are refreshed through the
      # chart importer, which does. Without this an instance that already has the
      # bundle's charts keeps whatever query_context they were created with —
      # exactly the state verify_superset() below fails on.
      charts_zip="$(mktemp --suffix=.zip)"
      if bundle_charts_zip "$zip" "$charts_zip"; then
        cbody="$(mktemp)"
        ccode="$(curl -s --max-time 300 -o "$cbody" -w '%{http_code}' \
            -X POST "$SUP_URL/api/v1/chart/import/" \
            -H "Authorization: Bearer ${tok}" -H 'Accept: application/json' \
            -F "formData=@$charts_zip" -F "passwords=$pw_json" -F 'overwrite=true' 2>/dev/null || echo 000)"
        if [[ "$ccode" == "200" ]]; then
          log "  refreshed $(basename "$zip") charts (HTTP 200, overwrite=true)"
        else
          warn "  chart refresh for $(basename "$zip") -> HTTP $ccode (charts kept their old query_context)"
          report_import_body "$cbody"
          rc=1
        fi
        rm -f "$cbody"
      else
        warn "  could not derive a chart copy of $(basename "$zip") — charts not refreshed"
        rc=1
      fi
      rm -f "$charts_zip"
    else
      warn "import $(basename "$zip") -> HTTP $code (nothing was imported: the request is atomic)"
      report_import_body "$body"
      rc=1
    fi
    rm -f "$body"
  done
  return $rc
}

sup_uuid_list() { # KIND(dashboards|charts|datasets) -> every uuid Superset already has
  case "$1" in
    dashboards) psql_q "$SUP_META_DB" "select uuid from dashboards" ;;
    charts)     psql_q "$SUP_META_DB" "select uuid from slices" ;;
    datasets)   psql_q "$SUP_META_DB" "select uuid from tables" ;;
  esac | tr 'A-Z' 'a-z'
}

check_zip_landed() { # ZIP — per-object: did every bundled object make it into Superset?
  local zip="$1" kind pairs uuids have total landed bad=0
  for kind in dashboards charts datasets; do
    pairs="$(zipquery "$zip" "uuids:$kind")"
    total="$(printf '%s\n' "$pairs" | grep -c . || true)"
    [[ "${total:-0}" -gt 0 ]] || continue
    uuids="$(printf '%s' "$pairs" | cut -f1)"
    have="$(sup_uuid_list "$kind")"
    landed="$(count_matches "$uuids" "$have")"
    if [[ "$landed" == "$total" ]]; then
      log "  $(basename "$zip"): $landed/$total $kind present in Superset"
    else
      warn "$(basename "$zip"): only $landed/$total $kind present — MISSING:"
      missing_of "$uuids" "$have" | while IFS= read -r u; do
        [[ -n "$u" ]] || continue
        printf '%s\n' "$pairs" | grep -F "$u" | cut -f2 | sed 's/^/      /'
      done
      bad=1
    fi
  done
  return $bad
}

verify_superset() { # 0 = the metaschema passes the gate's own dashboard assertions
  local n_dash n_charts n_null_ds n_null_qc per dead bad=0
  n_dash="$(psql_q "$SUP_META_DB" 'select count(*) from dashboards')"
  n_charts="$(psql_q "$SUP_META_DB" 'select count(*) from slices')"
  n_null_ds="$(psql_q "$SUP_META_DB" 'select count(*) from slices where datasource_id is null')"
  n_null_qc="$(psql_q "$SUP_META_DB" 'select count(*) from slices where query_context is null')"
  per="$(psql_q "$SUP_META_DB" "select string_agg(t, '  ' order by t) from (select d.id || '=' || count(*) || ' ' || d.dashboard_title as t from dashboard_slices ds join dashboards d on d.id = ds.dashboard_id group by d.id, d.dashboard_title) s")"

  log "superset meta: ${n_dash:-0} dashboards, ${n_charts:-0} charts"
  echo "        per dashboard: ${per:-none}"

  if [[ "${n_dash:-0}" -lt "$DASH_MIN" ]]; then
    warn "dashboards: ${n_dash:-0} (< $DASH_MIN) — the scripted seed (superset/setup.py + create_*)"
    warn "  has not run since the marts appeared; re-run its service:"
    warn "    docker compose -f superset/compose.yaml up -d --force-recreate superset-setup"
    warn "  (the zip import cannot make up this shortfall — it only adds its own dashboards)"
    bad=1
  else
    log "dashboards: ${n_dash:-0} (>= $DASH_MIN)"
  fi
  if [[ "${n_null_ds:-0}" -eq 0 ]]; then
    log "every chart has a datasource_id"
  else
    warn "$n_null_ds charts have no datasource_id — they cannot render"
    bad=1
  fi
  if [[ "${n_null_qc:-0}" -eq 0 ]]; then
    log "every chart has a query_context"
  else
    dead="$(psql_q "$SUP_META_DB" "select string_agg(t, ', ') from (select slice_name as t from slices where query_context is null order by slice_name limit 8) s")"
    warn "$n_null_qc charts have no query_context — those tiles fail with"
    warn "  'Chart has no query context saved. Please save the chart again.'"
    warn "  first 8: ${dead:-?}"
    bad=1
  fi
  return $bad
}

# ------------------------------------------------------------
# superset_dashboards — 0 = imported and complete, or legitimately deferred;
# 1 = the import ran and left Superset incomplete.
# ------------------------------------------------------------
superset_dashboards() {
  local zip rc=0
  [[ -n "$DASH_DIR" ]] || DASH_DIR="${INSTALL_DIR}/superset/dashboards"
  if ! compgen -G "$DASH_DIR/*.zip" >/dev/null 2>&1; then
    log "no bundled dashboards — nothing to import"
    return 0
  fi
  if ! command -v docker >/dev/null 2>&1; then
    warn "docker missing — cannot reach Superset"
    return 1
  fi
  if ! docker inspect -f '{{.State.Running}}' postgres 2>/dev/null | grep -q true; then
    warn "postgres is not running — dashboards NOT imported; re-run:"
    warn "  bash ${INSTALL_DIR}/install.sh --dashboards-only"
    return 0
  fi
  if ! curl -sf --max-time 5 "$SUP_URL/health" -o /dev/null 2>/dev/null; then
    warn "Superset not healthy at $SUP_URL — dashboards NOT imported; re-run:"
    warn "  bash ${INSTALL_DIR}/install.sh --dashboards-only"
    return 0
  fi
  if ! wait_for_marts; then
    warn "deferred: importing now would ship charts whose datasets have no tables behind them"
    warn "  re-run once the first transform has landed, on this host:"
    warn "    bash ${INSTALL_DIR}/install.sh --dashboards-only"
    return 0
  fi
  import_dashboards || rc=1
  for zip in "$DASH_DIR"/*.zip; do
    [[ -f "$zip" ]] || continue
    check_zip_landed "$zip" || rc=1
  done
  verify_superset || rc=1
  return $rc
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
  echo "  3. The bundled dashboard import defers until the marts exist (it is"
  echo "     skipped while the mart schema is empty). Once the DAG has run:"
  echo "       bash ${INSTALL_DIR}/install.sh --dashboards-only"
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
  # CONF_DIR:   runtime data directory — inside the repo dir (repo/_conf)
  #             (e.g. repo at /opt/data-lab → conf at /opt/data-lab/_conf).
  IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}') || IP="127.0.0.1"
  TZ_VAL=$(cat /etc/timezone 2>/dev/null | head -1) || TZ_VAL="America/New_York"
  DOCKER_GID=$(getent group docker | cut -d: -f3) || DOCKER_GID="999"
  CONF_DIR="$INSTALL_DIR/_conf"
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

  # --- Seed _conf/ and start stacks -----------------------------------------
  # init.sh creates all runtime directories under CONF_DIR and copies seed
  # config files (superset_config.py, meltano.yml, etc.) into place so
  # containers have their initial configuration on first boot.
  log "Seeding _conf/ directories..."
  bash init.sh

  # start.sh brings up every Docker Compose stack in the correct dependency
  # order (postgres first, then all dependents).
  log "Starting all stacks..."
  bash start.sh

  # --- Adopt stacks into Dockhand ------------------------------------------
  adopt_stacks

  # --- Auto-import Superset dashboards (background) ------------------------
  # superset init takes 20-40 min on first run, so the wait runs in the
  # background. install.sh exits after printing the service table; the import
  # completes on its own and logs to SUPERSET_LOG.
  #
  # The import is gated on the marts (see superset_dashboards) and normally
  # DEFERS here: the first `transform` is an Airflow run away — the operator has
  # not even triggered the DAG yet — so importing the bundle now would land its
  # charts with no tables behind them. Deferred is not a failure and this
  # background job cannot carry the install's exit status; the verdict belongs
  # to `--dashboards-only`, the re-run the deferral names.
  local SUPERSET_LOG=/tmp/superset-import.log
  (
    if wait_for_superset; then
      if superset_dashboards; then
        log "Dashboard import complete."
      else
        warn "Dashboard import INCOMPLETE — see the report above."
        warn "  re-run once the marts exist: bash ${INSTALL_DIR}/install.sh --dashboards-only"
      fi
    fi
  ) >> "$SUPERSET_LOG" 2>&1 &
  log "Superset dashboard import running in background — tail $SUPERSET_LOG to monitor"

  # --- Done ----------------------------------------------------------------
  print_services "$IP"
}

# ============================================================
# --dashboards-only — re-run just the bundled-dashboard import against an
# already-installed host (no clone, no start.sh, no prompts). This is the
# re-run the deferred message points at, and it is where the import's verdict
# decides an exit status: `err` (exit 1) when the import ran and left Superset
# incomplete, `DASH_STRICT=0` to downgrade that to a warning. Idempotent
# (overwrite=true), so it is safe to repeat.
#
# install.sh has no state file and no machine claim to fold the verdict into —
# and the install path's import runs in a background subshell, so the install's
# own exit status cannot carry it. This mode is the single place that can, which
# is why the verdict lives here instead of a stray `exit 1` at the bottom.
# ============================================================
if [[ "$DASHBOARDS_ONLY" == "1" ]]; then
  # No clone in this mode: use the tree we were pointed at, else the fallback.
  [[ -d "$INSTALL_DIR" || ! -d "$FALLBACK_DIR" ]] || INSTALL_DIR="$FALLBACK_DIR"
  [[ -d "$INSTALL_DIR" ]] || err "no installed tree at ${INSTALL_DIR} (set INSTALL_DIR=/path/to/data-lab)"
  cd "$INSTALL_DIR"
  command -v docker >/dev/null 2>&1 || err "docker not found on this host"
  log "Superset dashboards only (--dashboards-only) in ${INSTALL_DIR}"
  DASH_BAD=0
  superset_dashboards || DASH_BAD=1
  if [[ "$DASH_BAD" == "1" && "$DASH_STRICT" == "1" ]]; then
    err "Superset dashboards incomplete — see the report above."
  fi
  if [[ "$DASH_BAD" == "1" ]]; then
    warn "Superset dashboards incomplete — DASH_STRICT=0, so this run still exits 0."
  fi
  log "dashboards-only complete."
  exit 0
fi

main "$@"
