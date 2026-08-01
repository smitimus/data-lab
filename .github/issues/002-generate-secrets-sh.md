# Add generate-secrets.sh for existing clones

## Problem
`install.sh` is the only way to generate required secrets (Airflow Fernet key, JWT secret, shared session key, Superset secret key, Dockhand encryption key). It bundles secret generation with Docker installation, git clone, and full stack setup. Existing clones (e.g., after `git clone`) have no way to generate secrets without re-running the full installer.

## Evidence
During the 2026-08-01 dev deploy on VM 111, the Airflow stack failed with `ValueError: The value api_auth/jwt_secret must be set!`. The `fill_env()` function in `install.sh` would have generated this, but `install.sh` had never been run because the repo was already cloned.

## Proposed Solution
Extract the secret generation logic from `install.sh` into a standalone `generate-secrets.sh` that:
1. Reads `global.env` for existing values
2. Generates missing secrets using the same `generate_secret()` function
3. Writes them to the appropriate `.env` files
4. Is safe to re-run (idempotent — doesn't overwrite existing secrets)

## Affected Files
- `install.sh` — refactor to call `generate-secrets.sh`
- `airflow/.env` — needs `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, `AIRFLOW_SECRET_KEY`
- `superset/.env` — needs `SUPERSET_SECRET_KEY`
- `dockhand/.env` — needs `ENCRYPTION_KEY`

## Labels
enhancement, data-lab, security, startup

## Related
- `start.sh --continue-on-error` (separate ticket)
