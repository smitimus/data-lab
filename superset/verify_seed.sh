#!/usr/bin/env bash
# verify_seed.sh — wipe-proof verification of the Superset seed after a reseed.
# Usage: bash verify_seed.sh
# Steps:
#   1. Re-run superset-setup (idempotent seed)
#   2. Poll until it exits
#   3. Check seed log for failures
#   4. DB checks: dashboards=11, chart counts per dashboard, NULL datasource/query_context, bar-on-snapshot
#   5. Optional: render-check via authenticated API (chart/data) with throttle

set -uo pipefail
COMPOSE=/opt/data-lab/superset/compose.yaml

echo "=== [1] Re-seeding (idempotent) ==="
docker rm superset-setup 2>/dev/null >/dev/null
docker compose -f "$COMPOSE" up -d --force-recreate superset-setup >/dev/null 2>&1

echo "=== [2] Waiting for superset-setup to finish ==="
for i in $(seq 1 60); do
    ST=$(docker inspect superset-setup --format '{{.State.Status}}' 2>/dev/null)
    EX=$(docker inspect superset-setup --format '{{.State.ExitCode}}' 2>/dev/null)
    if [ "$ST" = "exited" ]; then echo "setup exited code=$EX after ${i}0s"; break; fi
    sleep 10
done

echo
echo "=== [3] Seed log failures ==="
FAILS=$(docker logs superset-setup 2>&1 | grep -cE "✗" || true)
echo "✗ lines: $FAILS"
docker logs superset-setup 2>&1 | grep -E "✗" | sort | uniq -c | sort -rn | head -10

echo
echo "=== [4] DB checks (superset meta) ==="
docker exec postgres psql -U postgres -d superset -t <<'SQL'
select 'dashboards: ' || count(*) from dashboards;
select 'charts: ' || count(*) from slices;
select 'per-dashboard: ' || dashboard_id || ' = ' || count(*)
  from dashboard_slices group by 1 order by 1;
select 'NULL datasource_id: ' || count(*) from slices where datasource_id is null;
select 'NULL query_context: ' || count(*) from slices where query_context is null;
SQL

echo
echo "=== charts per dashboard (expect: 1>=8, 2>=8, 3>=14, 4>=6, 5>=10, 6>=7, 7>=6, 8>=8, 9=8, 10>=8, 11>=9) ==="
