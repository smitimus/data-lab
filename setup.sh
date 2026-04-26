#!/bin/bash
# =============================================================================
# Post-first-boot setup: bulk-adopt all stacks into Dockhand.
# Run once on a fresh deploy after start.sh has finished.
# =============================================================================

# Exit immediately if any command fails.
set -e

# Resolve the repo root from this script's own location.
STACKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Read the server IP from global.env — the same IP used in all service .env
# files.  This avoids hard-coding the address and stays in sync with the rest
# of the stack configuration.
IP=$(grep '^IP=' "$STACKS/global.env" | cut -d= -f2)
DOCKHAND_URL="http://$IP:3000"

# -----------------------------------------------------------------------------
# wait_for NAME URL — polls the given URL every 3 seconds until it returns
# HTTP 200 or the attempt limit (30 tries = 90 seconds) is reached.
# Used to hold the script until Dockhand is ready to receive API requests.
# Prints a warning and continues rather than failing if the timeout is hit.
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
# Dockhand manages Docker Compose stacks through a web UI.  "Adopting" a stack
# registers its compose.yaml path with Dockhand so it appears in the UI and
# can be started, stopped, and monitored from there.
# adopt.py discovers all compose.yaml files under the STACKS directory and
# calls Dockhand's bulk-adopt API endpoint in a single request.

echo ""
echo "=== Dockhand — adopt stacks ==="

# Wait until Dockhand's web UI is serving requests before attempting the
# API call — the container may still be initializing its database.
wait_for "Dockhand" "$DOCKHAND_URL"

# Prompt for Dockhand credentials.  Press Enter to accept the defaults.
# The password prompt uses -s to suppress echo so the password is not
# visible in the terminal.
read -r -p "Dockhand admin username [admin]: " DH_USER
DH_USER=${DH_USER:-admin}
read -r -s -p "Dockhand admin password [admin]: " DH_PASS
DH_PASS=${DH_PASS:-admin}
echo ""

# Run adopt.py, passing the STACKS path as an environment variable.
# adopt.py reads STACKS to know which directory to scan for compose.yaml
# files; without it, the script would fall back to the hardcoded default
# /opt/stacks which may not match this installation's actual path.
STACKS="$STACKS" python3 "$STACKS/dockhand/adopt.py" --username "$DH_USER" --password "$DH_PASS"

# =============================================================================
# Done
# =============================================================================

echo ""
echo "=== Setup complete ==="
echo ""
echo "  Dockhand: $DOCKHAND_URL"
echo ""
