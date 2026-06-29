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
