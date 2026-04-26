#!/bin/bash
# =============================================================================
# Stack Deploy Init Script
# Run on a fresh machine (or to wipe and reinitialize conf/).
# Prompts to delete existing conf/ directories before recreating them.
# After seeding, use start.sh / stop.sh to bring stacks online/offline.
# =============================================================================

# Exit immediately if any command fails.
set -e

# Derive absolute paths from this script's own location so the script works
# regardless of where the repo was cloned (e.g. /opt/data-lab or /opt/stacks).
#   STACKS = the repo root directory (where this script lives)
#   CONF   = the runtime data directory, always a sibling of the repo
#            (e.g. repo at /opt/data-lab → conf at /opt/conf)
STACKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF="$(dirname "$STACKS")/conf"

# -----------------------------------------------------------------------------
# confirm MSG — prompts the user with MSG [Y/n] and returns 0 (yes) or 1 (no).
# Default answer is yes, so pressing Enter without typing accepts.
# -----------------------------------------------------------------------------
confirm() {
    local msg=$1
    read -r -p "$msg [Y/n] " resp
    case "$resp" in
        [nN]*) return 1 ;;
        *) return 0 ;;
    esac
}

# -----------------------------------------------------------------------------
# Check for existing conf/ directories
# -----------------------------------------------------------------------------
# Build a list of any conf/ subdirectories that already exist.  If the list
# is non-empty it means a previous deploy has left data behind; the user is
# prompted to choose between a clean wipe or an in-place re-seed.
EXISTING_DIRS=()
for d in airflow postgres superset meltano cloudbeaver homepage \
          dockhand; do
    [ -d "$CONF/$d" ] && EXISTING_DIRS+=("$CONF/$d")
done

if [ ${#EXISTING_DIRS[@]} -gt 0 ]; then
    echo ""
    echo "=== Existing conf/ directories detected ==="
    for d in "${EXISTING_DIRS[@]}"; do echo "  $d"; done
    echo ""
    if confirm "Delete all existing conf/ directories and do a clean init?"; then
        # Wipe selected: remove all listed dirs so the stack starts completely fresh.
        echo "  Removing conf/ directories..."
        for d in "${EXISTING_DIRS[@]}"; do
            rm -rf "$d"
            echo "  Removed: $d"
        done
    else
        # Keep selected: seed files will still be overwritten below, but
        # database files, logs, and other runtime data are preserved.
        echo "  Keeping existing conf/. Seeded files will be overwritten; runtime data (DBs, logs) preserved."
    fi
    echo ""
fi

# -----------------------------------------------------------------------------
# Create runtime directories
# -----------------------------------------------------------------------------
# These directories are mounted into containers as volumes.  They must exist
# on the host before `docker compose up` runs or Docker will create them as
# root-owned, which can cause permission errors inside some containers.
echo "=== Creating runtime directories ==="
mkdir -p $CONF/airflow/{logs,config,plugins,data}   # Airflow worker logs, DAG plugins, and data
chmod 777 $CONF/airflow/logs                        # airflow runs as UID 50000; needs to create log subdirs
mkdir -p $CONF/postgres/data                         # PostgreSQL data files (WAL, tables, indexes)
mkdir -p $CONF/superset/data                         # Superset cache and export scratch space
mkdir -p $CONF/dockhand                              # Dockhand app database (SQLite)

# -----------------------------------------------------------------------------
# Seed config files into conf/
# -----------------------------------------------------------------------------
# Several services need config files present before the container starts.
# These files live in the repo (stacks/) and are copied to conf/ so that:
#   1. The repo copy can be updated and re-seeded without touching live data.
#   2. The running container has a stable, editable config it can write to.

echo "=== Seeding superset ==="
mkdir -p $CONF/superset
# pip-extra: world-writable dir so the non-root superset container user can install
# psycopg2-binary here at init time; PYTHONPATH in compose.yaml points here so
# both superset-init and the main superset container find the package.
mkdir -p $CONF/superset/pip-extra && chmod 777 $CONF/superset/pip-extra
# superset_config.py — main Superset configuration (DB URI, secret key, feature flags).
cp $STACKS/superset/superset_config.py $CONF/superset/superset_config.py
# setup.py — Python script run by the superset-setup container on first boot
# to auto-provision the EDW database connection and import dashboards.
cp $STACKS/superset/setup.py $CONF/superset/setup.py

echo "=== Seeding meltano ==="
mkdir -p $CONF/meltano
# meltano.yml — declares the EL pipeline: tap-postgres (Verisim source)
# → target-postgres (EDW destination), including connection settings.
cp $STACKS/meltano/meltano.yml $CONF/meltano/meltano.yml

echo "=== Seeding cloudbeaver workspace ==="
mkdir -p $CONF/cloudbeaver/workspace/GlobalConfiguration/.dbeaver
# data-sources.json — pre-configures the EDW and Verisim database connections
# in CloudBeaver so they appear automatically on first login.
cp $STACKS/cloudbeaver/seed/GlobalConfiguration/.dbeaver/data-sources.json \
   $CONF/cloudbeaver/workspace/GlobalConfiguration/.dbeaver/data-sources.json

echo "=== Seeding homepage config ==="
mkdir -p $CONF/homepage
# Homepage (gethomepage.dev) reads YAML config files for its service tiles.
# The repo contains pre-configured tiles for every stack service.
cp $STACKS/homepage/config/* $CONF/homepage/

# -----------------------------------------------------------------------------
# Done
# -----------------------------------------------------------------------------
echo ""
echo "=== Init complete ==="
echo ""
echo "Run ./start.sh to bring all stacks online."
echo "Run ./stop.sh  to bring all stacks offline."
echo "Run ./setup.sh after first start to adopt stacks in Dockhand."
echo ""
