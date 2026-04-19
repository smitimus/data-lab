#!/bin/bash
# =============================================================================
# Stack Deploy Init Script
# Run on a fresh machine (or to wipe and reinitialize conf/).
# Prompts to delete existing conf/ directories before recreating them.
# After seeding, use start.sh / stop.sh to bring stacks online/offline.
# =============================================================================

set -e

CONF=/opt/conf
STACKS=/opt/stacks

# -----------------------------------------------------------------------------
# Prompt helper: ask yes/no, default yes
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
# Check for existing conf/ — prompt to wipe
# -----------------------------------------------------------------------------
EXISTING_DIRS=()
for d in airflow postgres superset meltano cloudbeaver homepage \
          openmetadata dockhand; do
    [ -d "$CONF/$d" ] && EXISTING_DIRS+=("$CONF/$d")
done

if [ ${#EXISTING_DIRS[@]} -gt 0 ]; then
    echo ""
    echo "=== Existing conf/ directories detected ==="
    for d in "${EXISTING_DIRS[@]}"; do echo "  $d"; done
    echo ""
    if confirm "Delete all existing conf/ directories and do a clean init?"; then
        echo "  Removing conf/ directories..."
        for d in "${EXISTING_DIRS[@]}"; do
            rm -rf "$d"
            echo "  Removed: $d"
        done
    else
        echo "  Keeping existing conf/. Seeded files will be overwritten; runtime data (DBs, logs) preserved."
    fi
    echo ""
fi

# -----------------------------------------------------------------------------
# Create runtime directories
# -----------------------------------------------------------------------------
echo "=== Creating runtime directories ==="
mkdir -p $CONF/airflow/{logs,config,plugins,data}
mkdir -p $CONF/postgres/data
mkdir -p $CONF/superset/data
mkdir -p $CONF/openmetadata/{opensearch,server}
mkdir -p $CONF/dockhand

# -----------------------------------------------------------------------------
# Seed config files
# -----------------------------------------------------------------------------
echo "=== Seeding superset ==="
mkdir -p $CONF/superset
cp $STACKS/superset/superset_config.py $CONF/superset/superset_config.py
cp $STACKS/superset/setup.py $CONF/superset/setup.py

echo "=== Seeding meltano ==="
mkdir -p $CONF/meltano
cp $STACKS/meltano/meltano.yml $CONF/meltano/meltano.yml

echo "=== Seeding cloudbeaver workspace ==="
mkdir -p $CONF/cloudbeaver/workspace/GlobalConfiguration/.dbeaver
cp $STACKS/cloudbeaver/seed/GlobalConfiguration/.dbeaver/data-sources.json \
   $CONF/cloudbeaver/workspace/GlobalConfiguration/.dbeaver/data-sources.json

echo "=== Seeding homepage config ==="
mkdir -p $CONF/homepage
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
