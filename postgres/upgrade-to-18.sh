#!/usr/bin/env bash
# =============================================================================
# postgres/upgrade-to-18.sh — PostgreSQL 16 -> 18 major-version upgrade
#
# For the shared EDW postgres service (postgres/compose.yaml).
# Strategy: pg_dump (custom format) from 16 -> restore into fresh 18 cluster.
#   - Dumps with the OLD binaries, restores with the NEW binaries (officially
#     supported cross-version path; format is forward-compatible).
#   - The data directory is NOT compatible across majors (16 -> 18 requires
#     dump/restore or pg_upgrade). This script uses dump/restore.
#
# Usage:  bash postgres/upgrade-to-18.sh
# Preflight: run from /opt/data-lab, postgres service running on 16.x
# Idempotent: safe to re-run after a partial failure (backup dir is reused).
# Rollback:  postgres/upgrade-to-18.sh --rollback
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."   # /opt/data-lab
CONF="${CONF:-/opt/data-lab/_conf}"
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_DIR="${CONF}/postgres/pg-upgrade-backup-${STAMP}"
DATA_DIR="${CONF}/postgres/data"
OLD_MAJOR="16"
NEW_MAJOR="18"
DBS="grocery superset airflow"

log()  { echo -e "\n[upgrade] $*"; }
die()  { echo -e "\n[upgrade] FATAL: $*" >&2; exit 1; }

rollback() {
  log "ROLLBACK: restoring PG ${OLD_MAJOR} data dir and compose pin"
  docker compose -f postgres/compose.yaml stop postgres || true
  LATEST=$(ls -d "${CONF}/postgres"/pg-upgrade-backup-* 2>/dev/null | sort | tail -1)
  [ -d "${LATEST}/data" ] || die "no backup data dir found under ${CONF}/postgres/pg-upgrade-backup-*"
  rm -rf "${DATA_DIR}"
  mv "${LATEST}/data" "${DATA_DIR}"
  sed -i "s|image: postgres:${NEW_MAJOR}|image: postgres:${OLD_MAJOR}|" postgres/compose.yaml
  docker compose -f postgres/compose.yaml up -d postgres
  log "Rollback done. Verify with: docker exec postgres psql -U postgres -c 'SELECT version();'"
  exit 0
}

[ "${1:-}" = "--rollback" ] && rollback

# ---------- preflight --------------------------------------------------------
command -v docker >/dev/null || die "docker not found"
docker compose -f postgres/compose.yaml ps postgres >/dev/null 2>&1 || die "postgres service not running (start stack first)"
CUR=$(docker exec postgres psql -U postgres -t -c "SHOW server_version;" | tr -d ' \n' | cut -d. -f1)
[ "$CUR" = "$OLD_MAJOR" ] || die "expected PG ${OLD_MAJOR} (got ${CUR}) — nothing to upgrade or wrong state"
FREE_GB=$(df -Pk . | awk 'NR==2 {print int($4/1024/1024)}')
[ "$FREE_GB" -ge 40 ] || die "need >=40GB free disk (got ${FREE_GB}GB) for dump + data-dir backup"
log "preflight OK — PG ${CUR}, free disk ${FREE_GB}GB"

# ---------- snapshot live counts for verification -----------------------------
SNAP="${BACKUP_DIR}/counts-before.txt"
mkdir -p "${BACKUP_DIR}"
for db in $DBS; do
  docker exec postgres psql -U postgres -d "$db" -t -c \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');" \
    | tr -d ' ' > "${BACKUP_DIR}/count-${db}.before"
done

# ---------- dump (old binaries, custom format, per-DB) ------------------------
log "dumping databases: $DBS (custom format) + globals"
docker exec postgres pg_dumpall -U postgres --globals-only > "${BACKUP_DIR}/globals.sql"
for db in $DBS; do
  docker exec postgres pg_dump -U postgres -Fc -d "$db" > "${BACKUP_DIR}/${db}.dump"
done
log "dumps written to ${BACKUP_DIR}"

# ---------- swap to PG 18 -----------------------------------------------------
log "stopping postgres (other containers keep running; consumers reconnect after full-stack restart)"
docker compose -f postgres/compose.yaml stop postgres
cp postgres/compose.yaml "${BACKUP_DIR}/compose.yaml.pg${OLD_MAJOR}"
mv "${DATA_DIR}" "${BACKUP_DIR}/data"        # keep old data dir for rollback
sed -i "s|image: postgres:${OLD_MAJOR}|image: postgres:${NEW_MAJOR}|" postgres/compose.yaml
grep -q "image: postgres:${NEW_MAJOR}" postgres/compose.yaml || die "compose pin bump failed"

log "starting postgres on ${NEW_MAJOR}"
docker compose -f postgres/compose.yaml up -d postgres
for i in $(seq 1 60); do
  docker exec postgres pg_isready -U postgres -q && break
  sleep 2
  [ "$i" = 60 ] && die "postgres:18 did not become ready"
done
docker exec postgres psql -U postgres -t -c "SHOW server_version;" | tr -d ' \n' | cut -d. -f1 | grep -q "^${NEW_MAJOR}" || die "postgres is not running ${NEW_MAJOR}"

# ---------- restore (new binaries read old-format dumps) ----------------------
log "restoring databases into PG ${NEW_MAJOR}"
docker exec -i postgres pg_restore -U postgres -d postgres < "${BACKUP_DIR}/globals.sql" \
  2> >(grep -v 'already exists' >&2) || true
for db in $DBS; do
  docker exec postgres psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 \
    || docker exec postgres createdb -U postgres "$db"
  docker exec -i postgres pg_restore -U postgres -d "$db" < "${BACKUP_DIR}/${db}.dump" \
    || die "restore failed for ${db} (dumps preserved in ${BACKUP_DIR})"
  log "  ${db}: restored"
done

# ---------- verify ------------------------------------------------------------
log "verifying: table counts before/after"
OK=1
for db in $DBS; do
  BEFORE=$(cat "${BACKUP_DIR}/count-${db}.before")
  AFTER=$(docker exec postgres psql -U postgres -d "$db" -t -c \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');" | tr -d ' ')
  echo "  ${db}: before=${BEFORE} after=${AFTER}"
  [ "$BEFORE" = "$AFTER" ] || OK=0
done
[ "$OK" = 1 ] || die "verification mismatch — check ${BACKUP_DIR}; rollback with --rollback"

# ---------- restart full stack so consumers reconnect cleanly -----------------
log "restarting full stack (airflow, superset, cloudbeaver reconnect to PG 18)"
bash stop.sh && bash start.sh

log "UPGRADE COMPLETE: PostgreSQL ${OLD_MAJOR} -> ${NEW_MAJOR}"
log "Backups (dumps + old data dir): ${BACKUP_DIR}"
log "Rollback if needed: bash postgres/upgrade-to-18.sh --rollback"
