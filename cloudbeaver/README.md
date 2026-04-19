# CloudBeaver

## Access
| Item     | Value                              |
|----------|------------------------------------|
| URL      | http://YOUR_SERVER_IP:8978         |
| Username | Set on first login                 |
| Password | Set on first login                 |
| Port     | 8978                               |

## What It Does
Web-based database IDE (DBeaver in a browser). Pre-configured with connections to all databases in the stack:
- **EDW** — `postgres:5432/edw` (postgres/postgres)
- **Verisim Grocery** — `verisim-grocery:5432/grocery` (verisim/verisim)
- **Airflow metadata** — `postgres:5432/airflow` (postgres/postgres)

## Key Config Files
- `conf/cloudbeaver/GlobalConfiguration/` — connection definitions (seeded from `stacks/cloudbeaver/data-sources.json` by `init.sh`)

## Usage Notes
- **First login:** CloudBeaver prompts to create an admin account on first visit. Choose any username/password.
- Connections are pre-loaded — expand the connection tree on the left to browse tables
- SQL editor: right-click a database → SQL Editor
- If connections disappear after a reset, re-run `init.sh` to reseed from `stacks/cloudbeaver/data-sources.json`
