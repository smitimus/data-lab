# AGENTS.md — Data Lab Analytics Stack

## Directory Layout

| Path | Purpose |
|------|---------|
| `/opt/data-lab/<service>/` | Deploy configs — compose.yaml, .env.example, README |
| `/opt/data-lab/_conf/<service>/` | Runtime data — config files, databases, logs. Seeded by init.sh |

**Self-contained stacks** (source mounted into container): airflow, postgres, verisim-grocery
**Seeded stacks** (init.sh copies stacks/ → _conf/): superset, cloudbeaver, homepage

## Stack Lifecycle

From `/opt/data-lab/`:

```bash
bash init.sh    # seed _conf/ from stacks/
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

dbt project: `/opt/data-lab/airflow/dbt/grocery/` (29 staging models, 14 mart models, 8 custom tests)

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

The dbt staging layer expects these 29 source tables from Verisim's generator:

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
| pos | returns | stg_pos_returns | Customer returns (refund ≤ txn total; 1 per txn) |
| pos | return_items | stg_pos_return_items | Return line detail (qty ≤ sold qty) |
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

These details come from the Verisim generator but are essential for dbt model work in this repo.

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

curl -s \
  -H "Authorization: Bearer ***" \
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
  │ (29 source tables → raw_* schemas via API)
  ▼
raw_hr, raw_pos, raw_timeclock, raw_ordering,
raw_fulfillment, raw_transport, raw_inv, raw_pricing
  │
  │ dbt run --select staging
  │ (29 SQL views, one per source table)
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

### Staging Layer (29 models)
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
| Pipeline run is stuck on `wait_for_verisim_readiness` | Is verisim still bootstrapping/backfilling? | `curl -s http://localhost:8010/grocery/status` (want `mode: realtime`) and `curl -s http://localhost:8010/grocery/stats/backfill-progress` (want `in_progress: false`) — the run resumes by itself once the source is ready |
| superset-init taking forever | Normal on first run | Wait 20-40 min; check `/tmp/superset-import.log` |
| Guest at `.x` behaves erratically — connection resets, timeouts, an apparent "sshd wedge" | **A duplicate vmid/MAC on another Proxmox host.** On 2026-09-21 vmid 107 existed on pve2 `.40` *and* on Betty `.10` at once, one MAC, one IP; sessions to `.7` flip-flopped between the two and one died mid-push | Look for the second copy **before rebooting anything**: `scripts/preflight-vmid.sh <vmid> --strict` in the `infra` repo (read-only, asks every host in `hosts.conf`). Rebooting the wrong copy is what made the original diagnosis worse — see `infra/slots.md` and kanban `t_9e2a355d` / `t_b1f18e60` |

## Pipeline Gotchas

- **DAGs start paused** — unpause before triggering (get a JWT first: `POST http://localhost:8080/auth/token`): `curl -X PATCH http://localhost:8080/api/v2/dags/<dag_id> -H 'Content-Type: application/json' -d '{"is_paused": false}'` with the `Authorization: Bearer <access_token>` header
- **The first pipeline run waits for verisim** — `grocery_complete_pipeline` begins with the `wait_for_verisim_readiness` sensor (`airflow/dags/verisim_readiness.py`), which holds `ingest` until the source API is healthy, the generator is in `realtime` mode (bootstrap + 30-day backfill finished), and **every source relation `grocery_ingest_api.TABLE_CONFIGS` loads** (32 tables) is served and — where the raw table does not exist yet — non-empty. The probe list is derived from the ingest DAG's own registry, so a table added to the ingest joins the gate in the same commit; a source that cannot serve one of them now fails *at the sensor* ("waiting for source relation online.orders …") instead of as five 404-ing ingest tasks (`t_17927141`, 2026-09-21). Unpausing the DAGs during provisioning is therefore safe: instead of failing once on `stg_pos_loyalty_point_transactions` (its raw table is never created when the endpoint returns no rows), the run waits for its input. Tuning: `VERISIM_READINESS_TIMEOUT_MIN` (default 120 min), `VERISIM_READINESS_POKE_S` (default 60 s)
- **dbt-docs**: runs `dbt docs generate` as same UID as Airflow. Don't change `user:` in dbt-docs/compose.yaml or PermissionError on logs/dbt.log
- **Ingest uses DROP TABLE ... CASCADE** for full-refresh tables because dbt staging views depend on raw tables; plain DROP TABLE raises DependentObjectsStillExist

## Archiving a Stack

1. Write `RESTORE.md` in the stack dir
2. `docker compose down`
3. `mv stacks/<name> archive/stacks/<name>` and `mv _conf/<name> archive/_conf/<name>`
4. Remove from: start.sh, stop.sh, init.sh (wipe-check list + mkdir + seed section), setup.sh
5. Update AGENTS.md active stacks list

## Init.sh Service Whitelist

init.sh only seeds _conf/ for these 6 services (hardcoded in the script):

```
superset cloudbeaver homepage postgres airflow verisim-grocery
```

If adding a new seeded stack, add its name to: the wipe-check list, the mkdir section, and the seed section in init.sh.
