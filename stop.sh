#!/bin/bash
# =============================================================================
# Stop all stacks (reverse start order).
# conf/ data is preserved — run init.sh to wipe and reinitialize.
# =============================================================================

# Resolve the repo root from this script's own location.
STACKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# stop NAME — runs `docker compose down` for the named stack.
#
# `docker compose down` stops and removes containers and networks but leaves
# named volumes and bind-mount data (in conf/) intact.  All persistent data
# (databases, logs, dashboards) survives a stop/start cycle.
stop() {
    local name=$1
    local dir=$STACKS/$name
    echo ""
    echo "--- $name ---"
    docker compose -f $dir/compose.yaml down
}

echo "=== Stopping all stacks ==="

# Stacks are stopped in reverse start order so that dependents are torn down
# before the services they depend on.  Stopping postgres before Airflow, for
# example, would cause Airflow to log connection errors during shutdown.

stop dbt-docs         # Data catalog — no other stack depends on it
stop homepage         # Landing page — no dependencies
stop cloudbeaver      # Database GUI — no dependencies
stop superset         # BI layer — depends on postgres (stopped later)
stop airflow          # Orchestrator — depends on postgres (stopped later)
stop meltano          # EL pipeline — depends on postgres (stopped later)
stop verisim-grocery  # Data source — independent, but stopped before postgres for clean ordering
stop postgres         # Shared EDW — stopped last; all dependent stacks are already down
stop dockhand         # Management UI — stopped last so it can observe the shutdown sequence

echo ""
echo "=== All stacks stopped ==="
echo ""
echo "conf/ data is preserved. Run ./start.sh to bring stacks back online."
echo "Run ./init.sh to wipe conf/ and do a clean reinitialize."
echo ""
