#!/bin/bash
# =============================================================================
# Post-first-boot setup: bulk-adopt all stacks into Dockhand.
# Run once on a fresh deploy after start.sh has finished.
# =============================================================================

set -e

STACKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IP=$(grep '^IP=' "$STACKS/global.env" | cut -d= -f2)
DOCKHAND_URL="http://$IP:3000"

# -----------------------------------------------------------------------------
# Wait for a service to become available
# -----------------------------------------------------------------------------
wait_for() {
    local name=$1
    local url=$2
    local max=30
    echo ""
    echo "--- Waiting for $name at $url ---"
    for i in $(seq 1 $max); do
        if curl -sf "$url" -o /dev/null 2>/dev/null; then
            echo "  $name is up."
            return 0
        fi
        echo "  ($i/$max) not ready yet..."
        sleep 3
    done
    echo "  WARNING: $name did not become ready in time. Continuing anyway."
}

# =============================================================================
# Dockhand — bulk adopt all stacks
# =============================================================================

echo ""
echo "=== Dockhand — adopt stacks ==="

wait_for "Dockhand" "$DOCKHAND_URL"

read -r -p "Dockhand admin username [admin]: " DH_USER
DH_USER=${DH_USER:-admin}
read -r -s -p "Dockhand admin password [admin]: " DH_PASS
DH_PASS=${DH_PASS:-admin}
echo ""

STACKS="$STACKS" python3 "$STACKS/dockhand/adopt.py" --username "$DH_USER" --password "$DH_PASS"

# =============================================================================
# Done
# =============================================================================

echo ""
echo "=== Setup complete ==="
echo ""
echo "  Dockhand: $DOCKHAND_URL"
echo ""
