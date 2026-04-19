# Meltano

## Access
| Item   | Value                          |
|--------|--------------------------------|
| Web UI | None (no web UI in Meltano v3.x) |
| Port   | 5000 (internal only)           |
| Auth   | None                           |

## What It Does
Singer-based Extract-Load tool. Extracts data from the verisim-grocery PostgreSQL database (source) and loads it into the `edw` database (target) in `raw_*` schemas.

- **tap:** `tap-postgres` targeting the verisim-grocery postgres (port 5499)
- **target:** `target-postgres` writing to the shared postgres EDW (port 5432)
- Extracts ~40 tables across all grocery schemas (hr, pos, timeclock, ordering, fulfillment, transport, inv, pricing, control)
- Maintains Singer state for incremental extraction

## Key Config Files
- `conf/meltano/meltano.yml` — tap/target configuration, schedules, state backend (seeded from `stacks/meltano/meltano.yml` by `init.sh`)

## Usage Notes
- **Not triggered manually** — Airflow calls Meltano via `docker exec meltano meltano run ...`
- **First start** installs Singer plugins (3–5 min). Check logs: `docker logs meltano -f`
- Plugin venvs are generated in `conf/meltano/.meltano/` — delete to force reinstall
- To run manually for debugging:
  ```bash
  docker exec meltano meltano run tap-postgres target-postgres
  ```
