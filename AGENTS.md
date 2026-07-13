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

**Fresh deploy** (repo already cloned): `bash init.sh && bash start.sh` then `bash setup.sh` (first-time only: adopt stacks in Dockhand).

**install.sh** uses `exec </dev/tty>` for interactive prompts — for non-interactive installs pipe a blank line: `echo '' | bash /tmp/install.sh`

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

## Source Tables Contract (verisim → dbt)

The dbt staging layer expects these 27 source tables from Verisim's generator:

| Source Schema | Table | dbt Staging Model | Notes |
|--------------|-------|-------------------|-------|
| hr | locations | stg_locations | Store/warehouse locations |
| hr | employees | stg_employees | Employee roster |
| hr | schedules | stg_hr_schedules | Shift schedules |
| pos | departments | stg_pos_departments | Product departments |
| pos | products | stg_pos_products | SKU catalog |
| pos | price_history | stg_pos_price_history | Price changes |
| pos | coupons | stg_pos_coupons | Active coupons |
| pos | combo_deals | stg_pos_combo_deals | Multi-product deals |
| pos | loyalty_members | stg_pos_loyalty_members | Loyalty program |
| pos | loyalty_point_transactions | stg_pos_loyalty_point_transactions | Points ledger |
| pos | transactions | stg_pos_transactions | POS header |
| pos | transaction_items | stg_pos_transaction_items | Line items |
| ordering | store_orders | stg_ordering_store_orders | Store replenishment orders |
| ordering | store_order_items | stg_ordering_store_order_items | Order line items |
| fulfillment | orders | stg_fulfillment_orders | Warehouse fulfillment |
| fulfillment | order_items | stg_fulfillment_items | Fulfillment line items |
| transport | trucks | stg_transport_trucks | Delivery truck fleet |
| transport | loads | stg_transport_loads | Truck dispatch records |
| transport | load_items | stg_transport_load_items | Load contents |
| inv | stock_levels | stg_inv_stock_levels | Inventory counts |
| inv | shrinkage_events | stg_inv_shrinkage_events | Loss/shrinkage |
| inv | receipts | stg_inv_receipts | Warehouse receipts |
| inv | receipt_items | stg_inv_receipt_items | Receipt line items |
| inv | products | stg_inv_products | Extended product data |
| pricing | weekly_ads | stg_pricing_weekly_ads | Ad circulars |
| pricing | ad_items | stg_pricing_ad_items | Ad line items |
| timeclock | events | stg_timeclock_events | Clock in/out, breaks |

**If a Verisim schema changes** (new column, renamed table), update the matching staging model in `/opt/data-lab/airflow/dbt/grocery/models/staging/` and the source definitions in `sources.yml`.

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
  -o superset/dashboards/export.zip
```

- **Import via API** (used in install.sh):

```bash
curl -s -X POST http://localhost:8088/api/v1/dashboard/import/ \
  -H "Authorization: Bearer $TOKEN" \
  -F "formData=@stacks/superset/dashboards/verisim_grocery_dashboards.zip" \
  -F 'passwords={"databases/Gas_Station.yaml":"postgres","databases/Grocery.yaml":"postgres"}'
```

## Pipeline Data Flow

```
verisim-grocery source DB (port 5499)
  │
  │ Airflow: grocery_ingest_api DAG
  │ (27 source tables → raw_* schemas via API)
  ▼
raw_hr, raw_pos, raw_timeclock, raw_ordering,
raw_fulfillment, raw_transport, raw_inv, raw_pricing
  │
  │ dbt run --select staging
  │ (27 SQL views, one per source table)
  ▼
staging (stg_*) — cleaned, typed, renamed columns
  │
  │ dbt run --select marts
  │ (14 materialized tables, business-domain aggregated)
  ▼
mart (mart_*) — revenue, labor, inventory, loyalty, products, etc.
  │
  ├── Superset dashboards (BI)
  ├── CloudBeaver (ad-hoc queries)
  └── dbt Docs (catalog + lineage + tests)
```

### Staging Layer (27 models)
- Raw → staging is **light** cleanup: column rename, type cast, COALESCE nulls
- Materialized as views (no storage cost)
- One view per source table (1:1 mapping)

### Mart Layer (14 tables)
- Materialized as tables (refreshed on each run)
- Domain-based: daily_revenue, department_performance, loyalty_cohort, etc.
- 7 custom data quality tests (freshness, uniqueness, referential integrity)

## Service Health Checks

```bash
# Postgres EDW
docker exec postgres pg_isready -U postgres -d grocery

# Verisim source DB
docker exec verisim-grocery pg_isready -U verisim -d grocery

# Airflow webserver + scheduler
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health

# Superset
curl -s -o /dev/null -w "%{http_code}" http://localhost:8088/api/v1/health

# dbt Docs
curl -s -o /dev/null -w "%{http_code}" http://localhost:8082/index.html

# CloudBeaver
curl -s -o /dev/null -w "%{http_code}" http://localhost:8978

# All containers healthy?
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -v Exited
```

## Troubleshooting Guide

| Problem | Check First | Fix |
|---------|------------|-----|
| Pipeline DAG fails on ingest | Is verisim-grocery running? | `docker ps | grep verisim-grocery` |
| Pipeline DAG fails on dbt | Is postgres EDW accessible? | `docker exec postgres psql -U postgres -d grocery -c "SELECT 1"` |
| Airflow DB migration hangs | First start, container exited | `docker compose -f /opt/data-lab/airflow/compose.yaml restart` |
| Superset shows no data | Has the pipeline run successfully? | Check mart tables: `docker exec postgres psql -U postgres -d grocery -c "SELECT COUNT(*) FROM mart.mart_daily_revenue"` |
| Container won't start | Port conflict? | `ss -tlnp | grep <port>` |
| dbt test fails | Schema mismatch? | Re-run ingest first, then dbt |
| superset-init taking forever | Normal on first run | Wait 20-40 min; check `/tmp/superset-import.log` |

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
