# OpenMetadata

## Access
| Item     | Value                              |
|----------|------------------------------------|
| URL      | http://YOUR_SERVER_IP:8585         |
| Username | admin                              |
| Password | admin                              |
| Port     | 8585                               |

## What It Does
Data catalog, lineage, and governance platform. Pre-seeded with service connections to all databases and the Airflow pipeline. Ingests dbt manifests for column-level lineage after the first Airflow DAG run.

## Key Config Files
- `openmetadata/compose.yaml` — OpenMetadata server + OpenSearch + setup container
- The setup container seeds service connections on first start

## Usage Notes
- **Startup time:** OpenMetadata server takes 2–3 minutes to become healthy. OpenSearch requires `vm.max_map_count=262144` (set by `install.sh`).
- **dbt lineage:** Run the Airflow grocery_pipeline DAG once first — it generates `manifest.json` in `conf/airflow/dbt/grocery/target/`. OpenMetadata ingests this on its next scheduled run.
- **Service connections** are seeded automatically by the setup container. If they're missing: Settings → Services → + New Service.
- **vm.max_map_count** is set in `/etc/sysctl.conf` by the installer. If OpenSearch fails to start: `sudo sysctl -w vm.max_map_count=262144`
