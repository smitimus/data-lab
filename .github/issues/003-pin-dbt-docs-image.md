# Pin dbt-docs image version instead of :latest

## Problem
`dbt-docs/compose.yaml` uses `ghcr.io/dbt-labs/dbt-postgres:latest`, which pulls whatever dbt version is current. When the dbt version changes, the `package-lock.yml` format may become incompatible, causing the container to crash-loop on startup.

## Evidence
During the 2026-08-01 dev deploy, dbt-docs pulled dbt 1.9.0 which rejected the project's existing `package-lock.yml` format with: `Validator Error: is not valid under any of the given schemas`. Deleting the lockfile fixed it, but this will break again on the next dbt version bump.

The `dbt-docs/Dockerfile` specifies `dbt-core==1.11.12` and `dbt-postgres==1.10.2` (pip packages), but the compose file ignores this Dockerfile and uses the public image directly. The public image tags don't match pip package versions.

## Proposed Solution
1. Determine the correct GHCR image tag that corresponds to the dbt version the project is tested against
2. Pin `compose.yaml` to that specific tag
3. Document the pinning in `AGENTS.md` so future upgrades are intentional
4. Alternatively: build from the local Dockerfile instead of using the public image

## Labels
bug, data-lab, dbt-docs, configuration

## Related
- None
