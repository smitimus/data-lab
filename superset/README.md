# Apache Superset

## Access
| Item     | Value                              |
|----------|------------------------------------|
| URL      | http://YOUR_SERVER_IP:8088         |
| Username | admin                              |
| Password | admin                              |
| Port     | 8088                               |

## What It Does
BI and dashboarding platform. Pre-loaded with dashboards built on the verisim grocery data marts. Connected to the `edw` PostgreSQL database.

## Key Config Files
- `conf/superset/superset_config.py` — Superset configuration (seeded from `stacks/superset/superset_config.py` by `init.sh`)
- `stacks/superset/dashboards/` — Exported dashboard JSON files for import

## Usage Notes
- **Dashboard import:** Superset auto-imports dashboards from `stacks/superset/dashboards/` during `install.sh`. Manual import: Dashboards → ⋮ → Import → select JSON file.
- **Adding a database connection:** Settings → Database Connections → + Database → PostgreSQL
  - Host: `postgres`, Port: `5432`, Database: `edw`, User: `postgres`, Password: `postgres`
- **First login:** admin / admin → prompted to change password (optional, skip for lab use)
- Data refreshes automatically as the Airflow DAG runs every 15 minutes
