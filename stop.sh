#!/bin/bash
# =============================================================================
# Stop all stacks (reverse start order).
# conf/ data is preserved — run init.sh to wipe and reinitialize.
# =============================================================================

STACKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

stop() {
    local name=$1
    local dir=$STACKS/$name
    echo ""
    echo "--- $name ---"
    docker compose -f $dir/compose.yaml down
}

echo "=== Stopping all stacks ==="

stop openmetadata
stop homepage
stop cloudbeaver
stop superset
stop airflow
stop meltano
stop verisim-grocery
stop postgres
stop dockhand

echo ""
echo "=== All stacks stopped ==="
echo ""
echo "conf/ data is preserved. Run ./start.sh to bring stacks back online."
echo "Run ./init.sh to wipe conf/ and do a clean reinitialize."
echo ""
