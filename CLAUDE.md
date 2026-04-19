# Claude Context — Data Lab (`stacks/`)

Full analytics engineering stack pre-wired to Verisim Grocery as the data source. The pipeline: verisim-grocery (source) → Meltano EL → postgres/edw → dbt → Superset + OpenMetadata.

## Directory Structure

| Directory | Purpose |
|-----------|---------|
| `stacks/<service>/` | Compose + .env.example + README for each service |
| `conf/<service>/` | Runtime data — config files, databases, logs, cache. Seeded by `init.sh` from `stacks/`. Safe to delete for a clean start. |

**Self-contained stacks** (source files in `stacks/`, mounted directly):
- `airflow` — `stacks/airflow/dags/` and `stacks/airflow/dbt/` mounted into container
- `postgres` — `stacks/postgres/init/` mounted as init scripts
- `verisim-grocery` — pulls `smiti/verisim-grocery` from Docker Hub

**Seeded stacks** (init.sh copies stacks/ → conf/ before first start):
- `superset`, `meltano`, `cloudbeaver`, `homepage`

## Stack Lifecycle Scripts

From `/opt/data-lab/` (or wherever the repo is cloned):

```bash
bash init.sh    # seed conf/ from stacks/ (prompts to wipe existing conf/)
bash start.sh   # start all stacks in dependency order
bash stop.sh    # stop all stacks in reverse order
bash setup.sh   # first-time only: bulk-adopt stacks in Dockhand
```

**Start order matters:** postgres first (creates postgres_network), then all dependents.
Changing start order breaks network dependencies.

## Environment Variables

- **Source of truth:** `stacks/global.env`
- **Sync:** `cd stacks && python global-env-sync.py` — pushes global vars to all service `.env` files
- **Rule:** Docker Compose only reads `.env` from the same directory as `compose.yaml`. Never use `env_file:` directive.
- **Override protection:** global-env-sync.py preserves lines with comments: `different`, `override`, `service-specific`, `custom`, `note`

## Docker Compose Conventions

- Extension: `.yaml` (not `.yml`)
- No `version:` field
- 2-space indentation
- Change notes at top of file: `# YYMMDD - description`
- Volume paths always use env vars: `${CONF}/service-name:/config`
- Homepage labels required for all web-facing services:
  ```yaml
  labels:
    - homepage.group=${HOMEPAGE_GROUP}
    - homepage.name=ServiceName
    - homepage.icon=service-name.png
    - homepage.href=http://${IP}:<port>/
    - homepage.description=Brief description — user/pass
  ```

## Running dbt

dbt runs inside the Airflow worker container:

```bash
docker exec airflow-worker bash -c \
  "cd /opt/airflow/dbt/grocery && dbt <cmd> --profiles-dir /opt/airflow/dbt --no-use-colors"
```

Common commands:
```bash
dbt run --select staging
dbt run --select marts
dbt test --select staging
dbt test --select marts
```

dbt project: `stacks/airflow/dbt/grocery/` (27 staging models, 14 mart models, 7 custom tests)
Profiles: `stacks/airflow/dbt/profiles.yml`

## PostgreSQL Access

```bash
docker exec postgres psql -U postgres -d edw -c "SELECT ..."
docker exec postgres psql -U postgres -d edw    # interactive
```

Verisim source DB (separate container):
```bash
docker exec verisim-grocery psql -U verisim -d grocery -c "SELECT ..."
```

## Grocery Data Model — Non-Obvious Business Logic

- **Transaction total**: `total = subtotal + tax - coupon_savings - deal_savings`
- **Line item total**: `line_total = (unit_price - discount) * quantity` — `discount` is per-unit
- **Timeclock events**: 4 types — `clock_in`, `clock_out`, `break_start`, `break_end`
- **`mart_loyalty_cohort.total_spend`**: nullable for members who signed up but never purchased

## Superset Dashboard Export/Import

Export (from running Superset):
```bash
TOKEN=$(curl -s -X POST http://localhost:8088/api/v1/security/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin","provider":"db"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8088/api/v1/dashboard/export/?q=!(1,2)" \
  -o stacks/superset/dashboards/export.zip
```

Import via API (used in install.sh):
```bash
curl -s -X POST http://localhost:8088/api/v1/dashboard/import/ \
  -H "Authorization: Bearer $TOKEN" \
  -F "formData=@stacks/superset/dashboards/verisim_grocery_dashboards.zip" \
  -F 'passwords={"databases/EDW.yaml":"postgres"}'
```

## Archiving a Stack

1. Write `RESTORE.md` in the stack dir
2. `docker compose down`
3. `mv stacks/<name> archive/stacks/<name>` and `mv conf/<name> archive/conf/<name>`
4. Remove from: `start.sh`, `stop.sh`, `init.sh` (wipe-check list + mkdir + seed section), `setup.sh`
5. Update CLAUDE.md active stacks list

## OpenMetadata Requirement

OpenSearch (used by OpenMetadata) requires:
```bash
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
```

The installer sets this automatically.
