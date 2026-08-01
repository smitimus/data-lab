# start.sh — Decouple stack failures from full pipeline abort

## Problem
`start.sh` uses `set -e` globally, meaning a single stack failure (e.g., Airflow apiserver crashing due to missing secrets) kills the entire startup sequence. Stacks after the failing one (superset, cloudbeaver, dbt-docs, minio) never start, even though they don't depend on the failed stack.

## Evidence
During the 2026-08-01 dev deploy, Airflow failed because `AIRFLOW_JWT_SECRET` was unset (only `install.sh` generates secrets). This caused `set -e` to abort the script after Airflow, leaving 4 stacks unstarted.

## Proposed Solution
Add a `--continue-on-error` flag that wraps each `start()` call with `|| true` or equivalent error handling, allowing the script to continue starting independent stacks even when one fails. Default behavior (no flag) should remain `set -e` for production deployments where a failed dependency should halt.

## Alternatives
- Per-stack error handling with `trap` and a summary report at the end showing which stacks failed
- Move `set -e` inside the `start()` function instead of at the script level

## Labels
enhancement, data-lab, startup

## Related
- `generate-secrets.sh` proposal (separate ticket) — would prevent the Airflow secret issue entirely
