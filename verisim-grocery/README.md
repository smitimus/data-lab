# Verisim Grocery

## Access
| Item         | Value                                                           |
|--------------|-----------------------------------------------------------------|
| Streamlit UI | http://YOUR_SERVER_IP:8501                                      |
| API docs     | http://YOUR_SERVER_IP:8010/docs                                 |
| PostgreSQL   | YOUR_SERVER_IP:5499 / user: verisim / pass: verisim / db: grocery |

## What It Does
Standalone grocery store mock data generator. Runs the verisim-grocery image as a single container — PostgreSQL + FastAPI + Streamlit UI + data generator managed internally by supervisord.

**The image is PINNED BY DIGEST** in `compose.yaml`, not by `:latest`:

    image: smiti/verisim-grocery@sha256:2900dbc0073…

A floating tag is not a version, and this stack's *content* is a data contract: the Airflow DAG
reads 32 named relations out of it, so an image that serves fewer of them does not error — the
readiness sensor holds the run and it looks like a hang. On 2026-09-21 dev (106) and test (107) ran
the same `:latest` tag with different bytes (dev a 07:45 build serving `online.orders` /
`pos.returns`; test the three-week-old 1.3.2 serving neither), five ingest tasks 404'd, and nothing
said so until the pipeline was already stuck (`t_9f544379`). The same class had bitten minio earlier
(a `:latest` namespace that had gone unpullable).

Moving the pin is a deliberate, reviewable edit to `compose.yaml`, made when a release exists in a
registry. `app-layer.sh` step 6b fetches and verifies the pin before `start.sh`, reports
`verisim=sha256:…` per slot in the deploy summary, and will not call a slot `provisioned` when its
running image is not the pinned one. The build pinned today is **not in a registry** (`docker pull`
of it is a 404): both slots hold it because it was side-loaded, carried as the dated tag
`smiti/verisim-grocery:dev-2026-09-21-0745`. A fresh slot cannot fetch it and will say so.

On first start, the generator auto-backfills 30 days of transaction history, then switches to real-time simulation (15-minute ticks). The Airflow pipeline reads from this database every 15 minutes.

**Streamlit UI tabs:** Dashboard (live metrics, auto-refresh) · Generator Control (start/stop/backfill) · Scenarios · Promotions (coupons, weekly ads) · Distributions · Table Explorer · Data Dictionary

## Key Config Files
- `verisim-grocery/compose.yaml` — pins the image **by digest** (`smiti/verisim-grocery@sha256:2900dbc…`,
  see above), exposes ports 5499, 8010, 8501
- **Networks:** the container sits on `verisim-grocery_default` plus `datalab_shared`.
  The Airflow worker joins `datalab_shared` to reach the source **by service name**
  (`verisim-grocery:8000` for the API, `:5432` for the DB) instead of the host IP.
  Never address the source by host IP from the pipeline: a stale `IP` value makes the
  ingest silently read a *different* instance's dataset (t_05b48b69, 2026-09-21).

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
