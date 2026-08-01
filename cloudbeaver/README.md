# CloudBeaver

## Access
| Item     | Value                              |
|----------|------------------------------------|
| URL      | http://YOUR_SERVER_IP:8978         |
| Username | admin                              |
| Password | admin                              |
| Port     | 8978                               |

## What It Does
Web-based database IDE (DBeaver in a browser). Pre-configured with connections to all databases in the stack:
- **EDW** — `postgres:5432/edw` (postgres/postgres)
- **Verisim Grocery** — `verisim-grocery:5432/grocery` (verisim/verisim)
- **Airflow metadata** — `postgres:5432/airflow` (postgres/postgres)

## Key Config Files
- `conf/cloudbeaver/GlobalConfiguration/` — connection definitions (seeded from `stacks/cloudbeaver/data-sources.json` by `init.sh`)

## Usage Notes
- **Admin credentials:** `admin` / `admin` (set via `CB_ADMIN_NAME` and `CB_ADMIN_PASSWORD` in compose.yaml)
- Connections are pre-loaded — expand the connection tree on the left to browse tables
- SQL editor: right-click a database → SQL Editor
- If connections disappear after a reset, re-run `init.sh` to reseed from `stacks/cloudbeaver/data-sources.json`

