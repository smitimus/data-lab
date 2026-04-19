# PostgreSQL

## Access
| Item     | Value                              |
|----------|------------------------------------|
| Host     | YOUR_SERVER_IP (or `postgres` on docker network) |
| Port     | 5432                               |
| Username | postgres                           |
| Password | postgres                           |

## What It Does
Shared PostgreSQL 16 database server. Hosts two databases:
- **`edw`** — Enterprise Data Warehouse. Contains `raw_*` schemas (loaded by Meltano), `staging` schema (dbt staging models), and `mart` schema (dbt mart models).
- **Airflow metadata** — Airflow scheduler and webserver state.

The verisim-grocery container has its **own separate postgres** on port 5499 — this shared postgres on port 5432 is the EDW only.

## Key Config Files
- `postgres/init/` — SQL init scripts (run once on first start, create `edw` database and schemas)
- `postgres/compose.yaml` — creates `postgres_network` shared by all other stacks

## Usage Notes
- Query the EDW from the host:
  ```bash
  docker exec postgres psql -U postgres -d edw -c "SELECT * FROM mart.mart_daily_revenue LIMIT 5;"
  ```
- No `psql` installed on the host — always use `docker exec postgres psql ...`
- The `pg` wrapper script at `~/bin/pg` is a convenience alias for interactive sessions
- PostgreSQL must start first — `start.sh` waits 30s after `docker compose up -d` for initialization
