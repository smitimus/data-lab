# AGENTS.md — Data Lab Analytics Stack

## Environment Naming

| Name | Machine | Purpose |
|------|---------|---------|
| **dev** | This machine (`/opt/data-lab`) | Active development |
| **test** | testvm (192.168.1.6) | Clean install validation |

## Directory Layout

| Path | Purpose |
|------|---------|
| `/opt/data-lab/<service>/` | Deploy configs — compose.yaml, .env.example, README |
| `/opt/conf/<service>/` | Runtime data — config files, databases, logs. Seeded by init.sh |

**Self-contained stacks** (source mounted into container): airflow, postgres, verisim-grocery
**Seeded stacks** (init.sh copies stacks/ → conf/): superset, cloudbeaver, homepage

## Stack Lifecycle

From `/opt/data-lab/`:

```bash
bash init.sh    # seed conf/ from stacks/
bash start.sh   # start all stacks (dependency order)
bash stop.sh    # stop in reverse order
bash setup.sh   # first-time only: adopt stacks in Dockhand
```

**Start order**: dockhand → homepage → postgres (wait 30s) → verisim-grocery → airflow → superset → cloudbeaver → dbt-docs

## Installer Tokens (`fill_env()` in install.sh)

| Token | Replaced with |
|-------|--------------|
| `YOUR_SERVER_IP` | Detected LAN IP |
| `YOUR_INSTALL_DIR` | Repo clone path |
| `YOUR_CONF_DIR` | conf path |
| `YOUR_TIMEZONE` | System timezone |
| `DETECT_ME_DOCKER_GID` | Docker group numeric GID |
| `GENERATE_ME_FERNET_KEY` | Airflow Fernet key |
| `GENERATE_ME_SECRET` | Shared Airflow + Superset session key |
| `GENERATE_ME_ENCRYPTION_KEY` | Dockhand encryption key |

## Environment Variables

- **Source of truth**: `/opt/data-lab/global.env`
- **Sync**: `cd /opt/data-lab && python3 global-env-sync.py` (pushes globals to all service .env files)
- **Rule**: Docker Compose only reads `.env` from the same dir as `compose.yaml`. Never use `env_file:` directive.
- **Override protection**: global-env-sync.py preserves lines with comments: `different`, `override`, `service-specific`, `custom`, `note`

## Docker Compose Conventions

- Extension: `.yaml` (not `.yml`)
- No version field, 2-space indentation
- Change notes at file top: `# YYMMDD - description`
- Volume paths use env vars: `${CONF}/<service>:/config`
- Homepage labels required for all web-facing services:

```yaml
labels:
  - homepage.group=${HOMEPAGE_GROUP}
  - homepage.name=ServiceName
  - homepage.icon=service-name.png
  - homepage.href=http://${IP}:<port>/
  - homepage.description=Brief description — user/pass
```

- `PGID` (not `GUID`) for group ID
- **Colon-in-label gotcha**: Values with `key: value` patterns break YAML parsing. Quote the whole label: `- "homepage.description=Grocery — UI: no auth"`

## Running dbt

dbt runs inside the Airflow worker container:

```bash
docker exec airflow-worker bash -c \
  "cd /opt/airflow/dbt/grocery && dbt <cmd> --profiles-dir /opt/airflow/dbt"
```

Common commands:
```bash
dbt run --select staging
dbt run --select marts
dbt test --select staging
dbt test --select marts
```

dbt project: `/opt/data-lab/airflow/dbt/grocery/` (27 staging models, 14 mart models, 7 custom tests)

## PostgreSQL Access

Shared EDW:
```bash
docker exec postgres psql -U postgres -d grocery -c "SELECT ..."
```

Verisim source DB (separate container):
```bash
docker exec verisim-grocery psql -U verisim -d grocery -c "SELECT ..."
```

## Grocery Data Model — Non-Obvious Business Logic

- **Transaction total**: `total = subtotal + tax - coupon_savings - deal_savings`
- **Line item total**: `line_total = (unit_price - discount) * quantity` — discount is per-unit
- **Timeclock events**: 4 types — `clock_in`, `clock_out`, `break_start`, `break_end`
- **`mart_loyalty_cohort.total_spend`**: nullable for members who signed up but never purchased

## Superset Quirks

- **First init** `superset init` takes 20–40 min — normal (role/permission sync)
- Container runs as non-root with `HOME=/app/superset_home`. Pip user-installs hit the bind-mounted volume and fail. Use `--target /tmp/pip-extra` + set `PYTHONPATH`
- **Export dashboards**:

```bash
TOKEN=$(curl -s -X POST http://localhost:8088/api/v1/security/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin","provider":"db"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8088/api/v1/dashboard/export/?q=!(1,2)" \
  -o stacks/superset/dashboards/export.zip
```

- **Import via API** (used in install.sh):

```bash
curl -s -X POST http://localhost:8088/api/v1/dashboard/import/ \
  -H "Authorization: Bearer $TOKEN" \
  -F "formData=@stacks/superset/dashboards/verisim_grocery_dashboards.zip" \
  -F 'passwords={"databases/Gas_Station.yaml":"postgres","databases/Grocery.yaml":"postgres"}'
```

## Pipeline Gotchas

- **DAGs start paused** — unpause before triggering: `curl -X PATCH http://localhost:8080/api/v2/dags/<dag_id> -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"is_paused": false}'`
- **First pipeline run on new install may fail** on `stg_pos_loyalty_point_transactions` (verisim still bootstrapping DB) — re-run passes
- **dbt-docs**: runs `dbt docs generate` as same UID as Airflow. Don't change `user:` in dbt-docs/compose.yaml or PermissionError on logs/dbt.log
- **Ingest uses DROP TABLE ... CASCADE** for full-refresh tables because dbt staging views depend on raw tables; plain DROP TABLE raises DependentObjectsStillExist

## Archiving a Stack

1. Write `RESTORE.md` in the stack dir
2. `docker compose down`
3. `mv stacks/<name> archive/stacks/<name>` and `mv conf/<name> archive/conf/<name>`
4. Remove from: start.sh, stop.sh, init.sh (wipe-check list + mkdir + seed section), setup.sh
5. Update AGENTS.md active stacks list

## Init.sh Service Whitelist

init.sh only seeds conf/ for these 6 services (hardcoded in the script):

```
superset cloudbeaver homepage postgres airflow verisim-grocery
```

If adding a new seeded stack, add its name to: the wipe-check list, the mkdir section, and the seed section in init.sh.
