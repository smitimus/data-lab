```markdown
# E2E Testing — Grocery Data Pipeline

End-to-end validation for the grocery analytics stack. Verifies data correctness from Verisim generation → Airflow ingestion → dbt transformation → Superset BI.

## Prerequisites

- All services running: postgres, verisim-grocery, airflow, superset
- Python 3 with: requests, psycopg2-binary
- PostgreSQL client (psql)

## Setup

cp .env.example .env
# Edit .env with your environment values

## Usage

bash e2e-test.sh

Exit 0 = all tests pass, 1 = any test fails.

## Test Phases

1. Pre-flight Checks — services healthy, APIs responsive
2. Generator Validation — source schemas exist, API returns data
3. Ingestion Validation — Airflow ingest DAG completes, 27 raw tables populated
4. dbt Staging — staging models run and pass tests
5. dbt Marts — mart models run and pass tests
6. Cross-Layer Consistency — row counts propagate, revenue matches
7. Superset Validation — dashboards have data
8. Report — per-phase PASS/FAIL summary

## Output

Each phase prints PASS/FAIL. Final line is OVERALL: PASS or FAIL.
Detailed logs written to LOG_DIR (default: /tmp/e2e-test-logs/).
```
