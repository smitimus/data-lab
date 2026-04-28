# Verisim Grocery

## Access
| Item         | Value                                                           |
|--------------|-----------------------------------------------------------------|
| Streamlit UI | http://YOUR_SERVER_IP:8501                                      |
| API docs     | http://YOUR_SERVER_IP:8010/docs                                 |
| PostgreSQL   | YOUR_SERVER_IP:5499 / user: verisim / pass: verisim / db: grocery |

## What It Does
Standalone grocery store mock data generator. Runs the full verisim-grocery Docker image from Docker Hub (`smiti/verisim-grocery:latest`) as a single container — PostgreSQL + FastAPI + Streamlit UI + data generator managed internally by supervisord.

On first start, the generator auto-backfills 30 days of transaction history, then switches to real-time simulation (15-minute ticks). The Airflow pipeline reads from this database every 15 minutes.

**Streamlit UI tabs:** Dashboard (live metrics, auto-refresh) · Generator Control (start/stop/backfill) · Scenarios · Promotions (coupons, weekly ads) · Distributions · Table Explorer · Data Dictionary

## Key Config Files
- `verisim-grocery/compose.yaml` — pulls `smiti/verisim-grocery:latest`, exposes ports 5499, 8010, 8501

## Usage Notes
- **This is the release-mode stack** — pulls from Docker Hub, no source code needed
- **For development** on the generator itself: use `verisim/switch.sh dev` from the verisim repo
- **Querying the source data:**
  ```bash
  docker exec verisim-grocery psql -U verisim -d grocery -c "SELECT COUNT(*) FROM pos.transactions;"
  ```
- **Check generator status:** Streamlit UI → Generator Control tab, or:
  ```bash
  curl http://localhost:8010/grocery/generator/status
  ```
- **Force backfill reset:**
  ```bash
  curl -X POST http://localhost:8010/grocery/generator/start \
    -H "Content-Type: application/json" \
    -d '{"mode":"backfill","force":true}'
  ```
- Meltano reads from this postgres (port 5499) — the shared EDW postgres (port 5432) is a separate container
