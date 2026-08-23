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
# IMPORTANT — postgres:18 image layout change (docker-library/postgres PR #1259):
#   The 18+ images store data at /var/lib/postgresql/<major>/docker and HARD-ERROR
#   if the legacy path /var/lib/postgresql/data exists as a mount point (even an
#   empty one). The supported config is a single mount at /var/lib/postgresql.
#   This script rewrites the volume line accordingly when swapping to 18.
#
# Usage:
#   bash postgres/upgrade-to-18.sh            # full upgrade: dump -> swap -> restore
#   bash postgres/upgrade-to-18.sh --resume   # continue a partial run (dumps exist,
#                                             # compose already on 18, old data moved)
#   bash postgres/upgrade-to-18.sh --rollback # restore 16 from the newest backup
# Preflight: run from /opt/data-lab (or anywhere; script resolves repo root).
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."   # /opt/data-lab
CONF="${CONF:-/opt/data-lab/_conf}"
STAMP="$(date +%Y%m%d-%H%M%S)"
DATA_DIR="${CONF}/postgres/data"
OLD_MAJOR="16"
NEW_MAJOR="18"
DBS="grocery superset airflow"
# legacy mount -> 18-style mount (single mount at /var/lib/postgresql)
OLD_MOUNT="- \${CONF}/postgres/data:/var/lib/postgresql/data"
NEW_MOUNT="- \${CONF}/postgres/data:/var/lib/postgresql"

log()  { echo -e "\n[upgrade] $*"; }
die()  { echo -e "\n[upgrade] FATAL: $*" >&2; exit 1; }

newest_backup() { ls -d "${CONF}/postgres"/pg-upgrade-backup-* 2>/dev/null | sort | tail -1; }

# ---------- rollback ----------------------------------------------------------
rollback() {
  log "ROLLBACK: restoring PG ${OLD_MAJOR} data dir and compose file"
  docker compose -f postgres/compose.yaml stop postgres || true
  LATEST=$(newest_backup)
  [ -n "$LATEST" ] || die "no backup dir found"
  [ -d "${LATEST}/data" ] || die "no data backup in ${LATEST}"
  rm -rf "${DATA_DIR}"
  mv "${LATEST}/data" "${DATA_DIR}"
  if [ -f "${LATEST}/compose.yaml.pg16" ]; then
    cp "${LATEST}/compose.yaml.pg16" postgres/compose.yaml   # full restore incl. mount + image
    log "compose restored from ${LATEST}/compose.yaml.pg16"
  else
    sed -i "s|image: postgres:${NEW_MAJOR}|image: postgres:${OLD_MAJOR}|" postgres/compose.yaml
    sed -i "s|${NEW_MOUNT}|${OLD_MOUNT}|" postgres/compose.yaml
  fi
  docker compose -f postgres/compose.yaml up -d postgres
  log "Rollback done. Verify with: docker exec postgres psql -U postgres -c 'SELECT version();'"
  exit 0
}

[ "${1:-}" = "--rollback" ] && rollback
RESUME=0
[ "${1:-}" = "--resume" ] && RESUME=1

# ---------- shared: bring up 18, restore, verify, restart ---------------------
restore_and_finish() {
  log "starting postgres on ${NEW_MAJOR}"
  docker compose -f postgres/compose.yaml up -d postgres
  for i in $(seq 1 90); do
    docker exec postgres pg_isready -U postgres -q 2>/dev/null && break
    sleep 2
    [ "$i" = 90 ] && die "postgres:${NEW_MAJOR} did not become ready — docker logs postgres"
  done
  docker exec postgres psql -U postgres -t -c "SHOW server_version;" | tr -d ' \n' | cut -d. -f1 \
    | grep -q "^${NEW_MAJOR}" || die "postgres is not running ${NEW_MAJOR}"

  log "restoring databases into PG ${NEW_MAJOR}"
  # globals.sql is plain SQL (pg_dumpall --globals-only) — restore with psql, not pg_restore.
  # "already exists" errors for the default postgres role are expected and harmless.
  docker exec -i postgres psql -U postgres -d postgres -v ON_ERROR_STOP=0 < "${BACKUP_DIR}/globals.sql" >/dev/null 2>&1 || true
  for db in $DBS; do
    docker exec postgres psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 \
      || docker exec postgres createdb -U postgres "$db"
    docker exec -i postgres pg_restore -U postgres -d "$db" < "${BACKUP_DIR}/${db}.dump" \
      || die "restore failed for ${db} (dumps preserved in ${BACKUP_DIR})"
    log "  ${db}: restored"
  done

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

  log "restarting full stack (airflow, superset, cloudbeaver reconnect to PG 18)"
  bash stop.sh && bash start.sh

  log "UPGRADE COMPLETE: PostgreSQL ${OLD_MAJOR} -> ${NEW_MAJOR}"
  log "Backups (dumps + old data dir): ${BACKUP_DIR}"
  log "Rollback if needed: bash postgres/upgrade-to-18.sh --rollback"
}

# ---------- resume a partial run ----------------------------------------------
if [ "$RESUME" = 1 ]; then
  BACKUP_DIR=$(newest_backup)
  [ -n "$BACKUP_DIR" ] || die "no pg-upgrade-backup-* dir found — nothing to resume"
  for f in grocery.dump superset.dump airflow.dump globals.sql; do
    [ -s "${BACKUP_DIR}/$f" ] || die "missing ${BACKUP_DIR}/$f — nothing to resume"
  done
  grep -q "image: postgres:${NEW_MAJOR}" postgres/compose.yaml \
    || die "compose is not pinned to ${NEW_MAJOR} — run WITHOUT --resume for a full upgrade"
  # 18-layout mount (idempotent; tolerates either form)
  sed -i "s|${OLD_MOUNT}|${NEW_MOUNT}|" postgres/compose.yaml
  grep -qE "/var/lib/postgresql$" postgres/compose.yaml || die "mount layout fix failed"
  # data dir must be empty (fresh cluster) — if a partial initdb wrote here, move it aside
  if [ -n "$(ls -A "${DATA_DIR}" 2>/dev/null)" ]; then
    log "data dir not empty — moving partial contents aside"
    mv "${DATA_DIR}" "${BACKUP_DIR}/data.partial"
  fi
  log "resuming from ${BACKUP_DIR}"
  restore_and_finish
  exit 0
fi

# ---------- full upgrade ------------------------------------------------------
# preflight
command -v docker >/dev/null || die "docker not found"
docker compose -f postgres/compose.yaml ps postgres >/dev/null 2>&1 || die "postgres service not running (start stack first)"
CUR=$(docker exec postgres psql -U postgres -t -c "SHOW server_version;" | tr -d ' \n' | cut -d. -f1)
[ "$CUR" = "$OLD_MAJOR" ] || die "expected PG ${OLD_MAJOR} (got ${CUR}) — nothing to upgrade or wrong state"
FREE_GB=$(df -Pk . | awk 'NR==2 {print int($4/1024/1024)}')
[ "$FREE_GB" -ge 40 ] || die "need >=40GB free disk (got ${FREE_GB}GB) for dump + data-dir backup"
log "preflight OK — PG ${CUR}, free disk ${FREE_GB}GB"

# snapshot live counts + dump (old binaries, custom format)
BACKUP_DIR="${CONF}/postgres/pg-upgrade-backup-${STAMP}"
mkdir -p "${BACKUP_DIR}"
for db in $DBS; do
  docker exec postgres psql -U postgres -d "$db" -t -c \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');" \
    | tr -d ' ' > "${BACKUP_DIR}/count-${db}.before"
done
log "dumping databases: $DBS (custom format) + globals"
docker exec postgres pg_dumpall -U postgres --globals-only > "${BACKUP_DIR}/globals.sql"
for db in $DBS; do
  docker exec postgres pg_dump -U postgres -Fc -d "$db" > "${BACKUP_DIR}/${db}.dump"
done
log "dumps written to ${BACKUP_DIR}"

# swap to PG 18: stop, move old data aside, bump image AND mount layout
log "stopping postgres (other containers keep running; consumers reconnect after full-stack restart)"
docker compose -f postgres/compose.yaml stop postgres
cp postgres/compose.yaml "${BACKUP_DIR}/compose.yaml.pg16"
mv "${DATA_DIR}" "${BACKUP_DIR}/data"        # keep old data dir for rollback
sed -i "s|image: postgres:${OLD_MAJOR}|image: postgres:${NEW_MAJOR}|" postgres/compose.yaml
sed -i "s|${OLD_MOUNT}|${NEW_MOUNT}|" postgres/compose.yaml
grep -q "image: postgres:${NEW_MAJOR}" postgres/compose.yaml || die "compose pin bump failed"
grep -qE "/var/lib/postgresql$" postgres/compose.yaml || die "mount layout fix failed (18+ requires mount at /var/lib/postgresql)"

restore_and_finish
