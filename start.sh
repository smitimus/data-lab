#!/bin/bash
# =============================================================================
# Start all stacks in dependency order.
# Run init.sh first on a fresh machine to seed conf/.
# =============================================================================

# Exit immediately if any command fails, so a broken stack doesn't silently
# let the rest of the sequence start against a broken dependency.
set -e

# Resolve the repo root from this script's own location.
# This ensures the script works whether the repo is at /opt/stacks,
# /opt/data-lab, or any other path.
STACKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# start NAME [build] — brings up a single Docker Compose stack.
#
# If "build" is passed as the second argument, the local image is rebuilt
# before starting.  This is used for stacks (like Airflow) that have a
# custom Dockerfile rather than pulling a pre-built image.
#
# `docker compose up -d` starts containers in detached mode so the terminal
# is not blocked waiting for container output.
start() {
    local name=$1
    local build=${2:-}
    local dir=$STACKS/$name
    echo ""
    echo "--- $name ---"
    if [ "$build" = "build" ]; then
        docker compose -f $dir/compose.yaml build --quiet
    fi
    docker compose -f $dir/compose.yaml up -d
}

echo "=== Starting all stacks ==="

# 1. Dockhand — Docker management UI.
#    Has no dependencies on other stacks; starts first so it is available
#    for monitoring while everything else comes up.
start dockhand

# 2. Postgres — shared EDW (Enterprise Data Warehouse) database.
#    Everything that stores persistent relational data depends on this:
#    Airflow (task metadata), Superset (dashboard definitions), Meltano
#    (pipeline state), and the grocery EDW itself.
#    The 30-second sleep gives postgres time to finish running its init
#    SQL scripts (creates airflow, superset, grocery databases) and begin
#    accepting connections before the dependent stacks try to connect.
start postgres
echo "  Waiting 30s for postgres to initialize..."
sleep 30

# 3. Verisim Grocery — mock grocery data generator.
#    All-in-one container (postgres + API + UI + generator).  Uses its own
#    internal postgres on port 5499 as the data source; independent of the
#    shared EDW postgres.
start verisim-grocery

# 4. Meltano — EL (Extract-Load) pipeline.
#    Extracts grocery transaction data from the Verisim source DB and loads
#    it into the EDW.  Installs its Singer tap/target plugins on first start
#    (takes 3-5 minutes — watch with: docker logs meltano-init).
start meltano

# 5. Airflow — workflow orchestrator.
#    Runs the grocery_pipeline DAG which calls Meltano (EL) and then dbt
#    (transform).  Built locally because the image includes dbt and its
#    dependencies baked in alongside the DAGs.
#    Runs DB migrations on first start (1-2 minutes).
start airflow build

# 6. Superset — BI dashboards.
#    The superset-init container runs database migrations and creates the
#    admin user before the main server starts.  On first run, `superset init`
#    (role/permission sync) can take 20+ minutes — this is normal.
#    The main superset container waits for superset-init to finish before
#    accepting HTTP requests.
start superset

# 7. CloudBeaver — web-based database GUI.
#    Pre-configured with connections to the EDW and Verisim source DB.
#    No hard startup dependencies on other stacks.
start cloudbeaver

# 8. Homepage — service dashboard / landing page.
#    Shows tiles for every running service with URLs and descriptions.
#    Config was seeded from stacks/homepage/config/ by init.sh.
start homepage

# 9. OpenMetadata — data catalog and lineage tracker.
#    Uses OpenSearch as its search and index backend, which requires the
#    vm.max_map_count kernel setting (set by install.sh).
#    Started last because it takes the longest to become healthy (2-3 min
#    for the server, plus the setup container seeds connections afterward).
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
