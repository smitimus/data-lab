#!/bin/bash
# =============================================================================
# Post-first-boot setup: bulk-adopt all stacks into Dockhand.
# Run once on a fresh deploy after start.sh has finished.
# Usage: bash setup.sh [-y|--yes]   (-y skips credential prompts, uses admin/admin)
# =============================================================================

# Exit immediately if any command fails.
set -e

AUTO_YES=false
for arg in "$@"; do
    [[ "$arg" == "-y" || "$arg" == "--yes" ]] && AUTO_YES=true
done

# Resolve the repo root from this script's own location.
STACKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Read IP from dockhand .env (already substituted by install.sh's fill_env).
# Fall back to detecting the LAN IP directly if the .env isn't present.
IP=$(grep '^IP=' "$STACKS/dockhand/.env" 2>/dev/null | cut -d= -f2 | sed 's/[[:space:]]*#.*//' | tr -d '[:space:]')
if [[ -z "$IP" || "$IP" == "YOUR_SERVER_IP" ]]; then
    IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}') || IP="127.0.0.1"
fi
DOCKHAND_URL="http://$IP:3000"

# -----------------------------------------------------------------------------
# wait_for NAME URL — polls the given URL every 3 seconds until it returns
# HTTP 200 or the attempt limit (30 tries = 90 seconds) is reached.
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

echo ""
echo "=== Dockhand — adopt stacks ==="

wait_for "Dockhand" "$DOCKHAND_URL"

if [[ "$AUTO_YES" == "true" ]]; then
    DH_USER="admin"
    DH_PASS="admin"
else
    read -r -p "Dockhand admin username [admin]: " DH_USER
    DH_USER=${DH_USER:-admin}
    read -r -s -p "Dockhand admin password [admin]: " DH_PASS
    DH_PASS=${DH_PASS:-admin}
    echo ""
fi

STACKS="$STACKS" python3 "$STACKS/dockhand/adopt.py" --username "$DH_USER" --password "$DH_PASS"

echo ""
echo "=== Setup complete ==="
echo ""
echo "  Dockhand: $DOCKHAND_URL"
echo ""
