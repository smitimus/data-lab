-- Analytics Engineer Stack — Central DW initialization
-- Creates metadata databases for Airflow and Superset.
-- Industry databases (gas_station, grocery, etc.) each contain raw_*, staging, mart schemas.
-- Meltano creates raw_* schemas on first load; dbt creates staging + mart on first run.
-- openmetadata database is created at runtime by the openmetadata-db-init container.

CREATE DATABASE airflow;
CREATE DATABASE superset;
CREATE DATABASE gas_station;
CREATE DATABASE grocery;
