# PostgreSQL

## Access
| Item     | Value                              |
|----------|------------------------------------|
| Host     | YOUR_SERVER_IP (or `postgres` on docker network) |
| Port     | 5432                               |
| Username | postgres                           |
| Password | postgres                           |

## What It Does
Shared PostgreSQL 16 database server. Hosts three databases:
- **`grocery`** — Enterprise Data Warehouse. Contains `raw_*` schemas (loaded by the `grocery_ingest_api` DAG), `staging` (dbt staging models), and `mart` (dbt mart models).
- **`airflow`** — Airflow scheduler and webserver metadata.
- **`superset`** — Superset metadata.

The verisim-grocery container has its **own separate postgres** on port 5499 — this shared postgres on port 5432 is the EDW only.

## Key Config Files
- `postgres/init/` — SQL init scripts (run once on first start, create `edw` database and schemas)
- `postgres/compose.yaml` — creates `postgres_network` shared by all other stacks

## Usage Notes
- Query the EDW from the host:
  ```bash
  docker exec postgres psql -U postgres -d grocery -c "SELECT * FROM mart.mart_daily_revenue LIMIT 5;"
  ```
- No `psql` installed on the host — always use `docker exec postgres psql ...`
- The `pg` wrapper script at `~/bin/pg` is a convenience alias for interactive sessions
- PostgreSQL must start first — `start.sh` waits 30s after `docker compose up -d` for initialization
