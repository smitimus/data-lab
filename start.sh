#!/bin/bash
# =============================================================================
# Start all stacks in dependency order.
# Run init.sh first on a fresh machine to seed conf/.
# =============================================================================

set -e

STACKS=/opt/stacks

start() {
    local name=$1
    local build=$2
    local dir=$STACKS/$name
    echo ""
    echo "--- $name ---"
    if [ "$build" = "build" ]; then
        docker compose -f $dir/compose.yaml build --quiet
    fi
    docker compose -f $dir/compose.yaml up -d
}

echo "=== Starting all stacks ==="

start dockhand
start postgres
echo "  Waiting 30s for postgres to initialize..."
sleep 30

start verisim-grocery
start meltano
start airflow build
start superset
start cloudbeaver
start homepage
start openmetadata

echo ""
echo "=== All stacks started ==="
echo ""
echo "Notes:"
echo "  - Meltano installs plugins on first start (3-5 min) — check: docker logs meltano-init"
echo "  - Airflow runs DB migrations on first start (1-2 min)"
echo "  - Superset auto-provisions the Gas Station dashboard after becoming healthy
  - verisim-grocery self-bootstraps its grocery DB on first start"
echo "  - OpenMetadata: server takes 2-3 min; setup seeds connections after server is healthy"
echo "  - OpenMetadata dbt pipeline: restart openmetadata-setup after first Airflow DAG run"
echo ""
