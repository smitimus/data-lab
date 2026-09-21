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

bash full-cycle.sh              # full wipe→reseed→start→pipeline→verify cycle (~90 min)
bash full-cycle.sh --no-wipe    # reuse existing _conf (start+pipeline+verify only)
bash full-cycle.sh --verify     # verification gates only (non-destructive)

**`full-cycle.sh` is the primary entrypoint** — it orchestrates the whole
fresh-wipe test including this script's data checks. Run it only when
explicitly requested; see the Hermes skill `e2e-testing` for the run protocol
and pass-invalidation rules.

bash test-full-cycle-guard.sh  # offline tests for the at-rest guard, the raw-layer
                                # recovery window and the mart verdict (stub docker for
                                # the guard + the EDW measurements; the control case
                                # measures the live instance)

bash test-mart-probe.sh         # the mart row probe against a real EDW: adds one empty
                                # mart relation and asserts the probe NAMES it instead of
                                # dying (t_0675c2ca). Needs the stack up; creates and drops
                                # mart.zz_probe_empty_tmp

bash test-install-dashboards.sh # offline tests for install.sh's bundled-dashboard import
                                # (stub docker/curl, synthetic bundle, no state touched)

bash e2e-test.sh                # data-correctness checks against a running stack

Exit 0 = all tests pass, 1 = any test fails.

### `full-cycle.sh` exit codes

| Code | Meaning |
|------|---------|
| 0 | every gate passed on an untouched run |
| 1 | a gate failed (see the phase output / log) |
| 2 | bad usage |
| 3 | **REFUSED — no data verdict produced** |

Exit 3 means no data verdict was produced, because the platform was in no state
to be judged:

- a tracked pipeline DAG run (`grocery_complete_pipeline`, `grocery_dbt`,
  `grocery_ingest_api`) was still running/queued, or a Superset re-seed was still
  running;
- the Airflow metadata db was unreadable, so the run state was unknown;
- **the raw layer was empty and the last load did not succeed** — the recovery
  window. `drop schema raw_* cascade` (airflow/README.md, "Source Addressing")
  plus the rebuild that follows leaves the platform legitimately empty with *no
  run in flight*: the raw layer is gone and the marts are half-built until the
  rebuild finishes. An empty layer behind a *successful* load is a real data
  finding and still FAILs.

Marts legitimately do not exist until `transform` completes, so `--verify`
refuses rather than reporting the pipeline's own progress as a data failure
("only 0 populated marts", "N charts missing query_context"). Wait for the runs
to finish (or for the rebuild to succeed) and re-run `--verify`.

## Which slot to run this on

`full-cycle.sh --verify` produces a verdict, so it belongs on the **test** slot
(CT107, 192.168.1.7): a verification loop must not share a host with the
development loop (dev, CT106 / 192.168.1.6).

- On the test slot it judges a revision that is already committed — confirm the
  checkout is at the revision you mean (`git -C /opt/data-lab log -1`), or the
  green verdict is for code nobody is shipping.
- A run on dev while an agent is editing, or while a loader is rebuilding the raw
  layer, is what produced the "raw empty-ish (0)" / "only 0 of 0 populated marts"
  verdict on 2026-09-21: a correct FAIL about a platform mid-rebuild, read as a
  data problem (t_657cebc3).
- The at-rest guard refuses for tracked DAG runs, and — since t_77ac6468 — for the
  recovery window too: an empty raw layer whose last load did not end in
  `success` is judged as "mid-rebuild", not as a data failure, so `drop schema
  raw_* cascade` in the gap between the drop and the rebuild no longer produces a
  verdict. (The guard reads the last ended `grocery_ingest_api` /
  `grocery_complete_pipeline` run from `dag_run`; a platform that is empty behind
  a *successful* load still FAILs — that is a data problem.)

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
