# Airflow

## Access
| Item     | Value                              |
|----------|------------------------------------|
| URL      | http://YOUR_SERVER_IP:8080         |
| Username | admin                              |
| Password | admin                              |
| Port     | 8080                               |

## What It Does
Orchestrates the data pipeline in two phases: (1) `grocery_ingest_api` — paginated HTTP ingestion from the Verisim API into EDW raw_* schemas, (2) `grocery_dbt` — dbt staging → marts → tests.

Built from a custom Dockerfile (`airflow/Dockerfile`) on top of `apache/airflow:3.1.3` with dbt-core, dbt-postgres, and the Docker CLI added.

## Key Config Files
- `airflow/dags/grocery_ingest_api.py` — API ingestion DAG (HTTP → raw_* schemas)
- `airflow/dags/grocery_dbt.py` — dbt transformation DAG (raw → staging → marts → tests)
- `airflow/dbt/grocery/` — dbt project (27 staging models, 14 mart models, 7 custom tests)
- `airflow/dbt/profiles.yml` — dbt connection profiles (grocery + gasstation)

## Usage Notes
- **First run:** Airflow runs DB migrations on startup (1–2 min). Wait for the webserver to show "healthy" before triggering DAGs.
- **Trigger manually:** Airflow UI → DAGs → grocery_pipeline → ▶ Trigger
- **dbt commands** must run inside the Airflow worker container:
  ```bash
  docker exec airflow-worker bash -c \
    "cd /opt/airflow/dbt/grocery && dbt run --profiles-dir /opt/airflow/dbt --no-use-colors"
  ```
- **DAG logs:** Airflow UI → DAGs → grocery_pipeline → click a run → click a task
- `DOCKER_GID` must match the host docker group GID — the installer detects this automatically

## Source Addressing (verisim → ingest)

The ingest DAG reaches the Verisim source **by Docker service name**, never through
the host's `IP`:

| Variable | Default |
|----------|---------|
| `VERISIM_API_URL` | `http://verisim-grocery:8000` |
| `VERISIM_DB_HOST` / `_PORT` / `_NAME` / `_USER` / `_PASSWORD` | `verisim-grocery` / `5432` / `grocery` / `verisim` / `verisim` |

Both resolve over the `datalab_shared` network: `verisim-grocery/compose.yaml` owns
it, this stack joins it as external (worker + scheduler). `start.sh` starts
verisim-grocery before airflow, so the network already exists.

Why this is a rule, not a preference: `IP` is a host-specific value baked into a
container's environment when the container is **created**. When it goes stale the
ingest does not error — it silently reads a *different instance's dataset* and
writes it into the EDW as if it were ours. That is t_05b48b69 (2026-09-21): a fresh
dev instance, whose worker still carried `IP=192.168.1.7` from the host being
replaced, ingested 1,136,360 foreign transactions against a 98,112-row local source
and then spent hours pulling that instance's 6.6M `transaction_items`. Service DNS
cannot drift: it resolves inside the stack or the task fails loudly.

## Ingest Invariant: `verify_raw_vs_source`

`grocery_ingest_api` ends with a `verify_raw_vs_source` task (trigger rule
`all_done`) that compares every raw table's row count against its source relation
and **fails the run when the EDW holds more rows than the source**.

- One-sided by design: the source only grows while a run is in flight, and the raw
  load is a primary-key upsert, so a faithful load can never exceed the source
  count. Any excess means a foreign instance, a reloaded source, or an append where
  an upsert was intended.
- Shortfalls are logged, not failed — `pos_coupons` and `pos_combo_deals` serve a
  subset of their relation (`active_only=True`), and partial loads are already fatal
  in `_assert_complete` (rows written vs the API's own advertised total).
- The raw→source mapping lives in `SOURCE_RELATIONS` in the DAG. Adding a table to
  `TABLE_CONFIGS` without adding it there **fails at DAG parse time**, so a new
  table cannot quietly go unreconciled.

`e2e-testing/full-cycle.sh --verify` re-checks the load-bearing tables from the host
(raw must not exceed source) as part of the acceptance gate.

