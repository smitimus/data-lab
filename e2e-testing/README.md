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

bash test-full-cycle-drain.sh    # offline tests for phase 6's slot drain: the drain of
                                # a scheduler-created catch-up run, the confirmation
                                # window that stops it racing the run it drains, the
                                # never-at-rest timeout and the unreadable-metadata-db
                                # refusal (stub docker for the `dag_run` read; the
                                # drain functions are extracted from the script, so
                                # this measures the shipped bytes; no state touched)

bash test-mart-probe.sh         # the mart row probe against a real EDW: adds one empty
                                # mart relation and asserts the probe NAMES it instead of
                                # dying (t_0675c2ca). Needs the stack up; creates and drops
                                # mart.zz_probe_empty_tmp

bash test-install-dashboards.sh # offline tests for install.sh's bundled-dashboard import and
                                # the two converges that follow it: the link reconcile, and
                                # the prune of the chart generation the import displaced
                                # (stub docker/curl, synthetic bundle, no state touched)

bash test-dataset-metadata-gate.sh # offline tests for superset/verify_dataset_metadata.sh —
                                # the stale-dataset gate (fixture lists through the gate's
                                # EDW_LIST/SUP_LIST/SUP_DS_LIST hooks; no state touched)

bash test-bind-dashboard-layout.sh # offline tests for superset/dashboards/bind_dashboard_layout.py —
                                # the export's bind + prune-to-closure pass: a retired
                                # dashboard takes its charts, their datasets and an
                                # unreferenced database with it (an orphan is never
                                # imported, and install.sh's check_zip_landed reports
                                # it MISSING); synthetic bundle, no state touched.
                                # Needs PyYAML — this one runs on the workstation with
                                # the tool, not on the slot

bash test-seed-chart-identity.sh # offline tests for the chart-identity rule in
                                # superset/create_missing_dashboards.py: the seed adopts an
                                # existing chart by NAME + DATASET, never by name alone —
                                # a same-named chart on another mart is refused with a
                                # warning, and the lookup pages past the 100-row page
                                # Superset actually serves. Drives the real module against
                                # a fake chart API; a red control runs the same driver
                                # against the pre-fix name-only lookup (t_0f87aab9)

bash test-secret-defaults.sh    # offline tests for global.env's shipped secret defaults:
                                # the three session/JWT keys are the GENERATE_ME_SECRET
                                # sentinel and the two key-shaped ones (Fernet, Dockhand)
                                # decode to data-lab-shipped-default-key-00N; no
                                # .env.example carries a concrete value; and in a temp
                                # copy of the tree generate-secrets.sh replaces the
                                # sentinel, leaves nothing sentinel-shaped in any .env,
                                # does NOT touch the key-shaped defaults, and warns about
                                # every rotation that destroys stored state — including
                                # SUPERSET_SECRET_KEY, which it does replace and which
                                # Superset also uses to decrypt its stored connection
                                # passwords (t_19e41e00). A red control reproduces the
                                # b18088e shape (a concrete value survives the generator)
                                # so the check is not vacuous (t_35b04ad9).
                                # Slot-aware: run in place on a live slot it asserts the
                                # COMMITTED global.env and says so — see "The
                                # shipped-defaults guard on a slot" below.

bash test-secret-defaults-slot-aware.sh # offline tests for that slot-awareness (t_5807e8f9):
                                # builds a slot-shaped tree (git index at the committed
                                # content, working global.env site-local) and asserts the
                                # in-place run PASSes 42 and names the substitution,
                                # writes nothing into the tree; that forcing the working
                                # tree still fails loudly; that a committed concrete
                                # value is still caught; that no live value is ever
                                # printed; and that the clean-checkout / git-archive
                                # invocations and the exit-2 usage errors hold

bash e2e-test.sh                # data-correctness checks against a running stack

Exit 0 = all tests pass, 1 = any test fails.

## The shipped-defaults guard on a slot (t_5807e8f9)

`global.env` is the one file in the tree that is legitimately host-local: a live
slot's working tree carries that host's rotated live secrets plus its own `IP` /
`HOMEPAGE_ALLOWED_HOSTNAMES`, while the committed file carries the shipped
defaults. The deploy-owned refresh names it `SITE_LOCAL`
(`infra/scripts/refresh-slot-tree.sh`) and preserves it byte-for-byte for exactly
that reason.

`test-secret-defaults.sh` guards the **committed** tree: part 1 asserts the five
shipped defaults, and parts 4 and 6 drive `generate-secrets.sh` and `install.sh`'s
patch against an offline copy of it. Asserting the working tree on a slot is
therefore a failure by construction — part 1 fails on the five site-local values
(5 assertions), part 4 follows because the generator correctly sees non-sentinel
values and generates nothing (4), and part 6 likewise (12): `FAIL (21 of 42)`,
measured on dev/106 and test/107 at 518523e. That verdict reads as "the refresh
broke the slot" and invites "repairing" the one file that must never be touched.

So the suite is slot-aware. When the working-tree `global.env` differs from the
committed one it prints one explicit note and asserts the committed copy (the git
index — what a commit right now would ship), which is what it guards:

```
$ bash /opt/data-lab/e2e-testing/test-secret-defaults.sh
=== global.env shipped secret defaults
tree: /opt/data-lab
global.env: the COMMITTED copy (git index) — the working tree differs; see the note below

note: /opt/data-lab/global.env differs from the committed one. On a live slot that is the
      site-local copy: it carries the host's rotated live secrets (and the host's
      IP / HOMEPAGE_ALLOWED_HOSTNAMES) and must not be touched — a refresh
      preserves it byte-for-byte. …
        working tree  sha256 a9803041d1f2a592…
        committed     sha256 fd0c54405c366fe9…
      This is expected on a slot. To assert the working tree instead:
        SECRET_DEFAULTS_SOURCE=worktree bash …/test-secret-defaults.sh
…
SECRET-DEFAULT TESTS: PASS (42 assertions)
```

The note is expected on a slot, and the file must **not** be edited to silence it.
In a clean checkout the two copies are the same bytes, so the run is unchanged:
42 assertions, no note. `bash e2e-testing/test-secret-defaults-slot-aware.sh`
asserts all of the above offline (41 assertions) — including that the in-place run
writes nothing into the tree it asserts, and that no live value is ever printed.
Knobs:

| knob | effect |
|------|--------|
| *(default)* | assert the committed `global.env` whenever it differs from the working tree; one note names the substitution |
| `SECRET_DEFAULTS_SOURCE=worktree` | assert the working tree as-is — a dev checking an uncommitted edit. On a slot this is the old `FAIL (21 of 42)` |
| `SECRET_DEFAULTS_SOURCE=committed` | assert the committed copy unconditionally (exit 2 outside a git work tree) |
| `--root <tree>` | assert a tree other than the one the script lives in, so a slot can be guarded from outside its own `/opt/data-lab`, writing nothing into it |

The `git archive HEAD | tar -x` form still works: outside a git work tree there is
nothing to compare against, the working tree is asserted, and in an archive that
*is* the committed file.

Nothing in the suite writes to the tree it asserts (every part runs on copies in a
temp dir), and a mismatch in any of the five secret values is reported as
`len=<n> sha256=<16 hex>`, never as the value — on a slot those bytes are the
host's live keys, and the in-place run this suite used to force echoed a live
`SUPERSET_SECRET_KEY` into the log and from there into the card that quoted it.
Exit codes: `0` all assertions hold, `1` at least one does not, `2` bad
usage/environment (unknown argument or `SECRET_DEFAULTS_SOURCE`, `--root` that is
not a directory, `committed` with no committed `global.env` to read).

**What a refresh card's acceptance should say.** The constraint belongs in the
card, so the next refresh does not re-derive it (there is no card template — the
acceptance text is written per card):

> `bash /opt/data-lab/e2e-testing/test-secret-defaults.sh` reports
> `PASS (42 assertions)` on both slots. On a slot it also prints the one
> "working-tree `global.env` differs from the committed one" note — expected,
> because the slot's `global.env` is site-local (preserved byte-for-byte by the
> refresh). Do **not** edit the slot's `global.env` to make the note go away: it
> carries the host's live rotated secrets and the running stack's
> `SUPERSET_SECRET_KEY`.

## The DOM gate — `dom-scan.js` (the final gate)

`full-cycle.sh` ends with the note that browser DOM verification of every
dashboard is the FINAL gate and is performed by the agent. That job is now a
script in this directory instead of an ad-hoc console snippet per worker.

**Why it exists:** an API 200 is not proof that a chart renders. Seeded charts
answer 200 while the browser shows `Columns missing in dataset` / `There is no
chart definition associated with this component…`; the parent gate found 39
broken tiles that every API and DB check reported green. The gate renders each
dashboard in a real headless Chromium over CDP and reads the DOM the user sees
(`.dashboard-component` text) — **not** a screenshot: ECharts canvases render
blank headless, while the error text is right there in the component.

It runs on the workstation, not on the slot (neither dev nor test has node or
Chromium); it drives the slot's Superset over HTTP.

```bash
bash e2e-testing/dom-scan.sh <test-slot>                 # every published dashboard
bash e2e-testing/dom-scan.sh <test-slot> data-quality-ops
bash e2e-testing/dom-scan.sh <test-slot> data-quality-ops,store_hr_labor,4
node e2e-testing/dom-scan.js <test-slot> --json /tmp/dom.json   # + machine-readable report
```

**Address dashboards by slug.** Ids drift between instances and reseeds
(`data-quality-ops` is dash 12 on test, dash 6 on dev), so the slug is the only
stable handle for a gate. With no dashboard argument the harness discovers every
published dashboard through the API and scans each by slug; a numeric id is
accepted and resolved through the API first. A dashboard whose slug is NULL
(dash 3 "Grocery Overview" on test) falls back to its id.

Exit codes:

| Code | Meaning |
|------|---------|
| 0 | every requested dashboard rendered with no error tile |
| 1 | error tiles found, or a dashboard that carries charts rendered none |
| 2 | **wrong page / unresolved target** — the false-pass guard |
| 3 | harness failure (host unreachable, no Chromium, CDP error, bad usage) |

Exit 2 or 3 is **not** a pass: nothing was certified. False-pass guards, each one
a failure mode seen on this stack:

- an absent/expired session lands on `/login/`, which has no `.dashboard-component`
  at all and therefore scans "clean" → failed loudly as exit 2;
- the SPA can settle on a *different* dashboard than the requested one, which also
  reports a clean count → the rendered `location.pathname` **and** `document.title`
  must both match the API-resolved target, or the scan retries and then exits 2;
- a dashboard the API says carries charts but whose page renders 0 components is
  the silent-skip symptom (9 charts skipped, exit 0) → reported as exit 1, not as
  a clean empty page;
- the error regex is the entire detection surface — a keyword missing from it is a
  false negative, not a pass. Current set: `Unexpected error`,
  `Columns missing in datas[oe]t`, `Columns missing in datasource`,
  `Datetime column not provided`, `Something went wrong`, `rolling window`,
  `chart definition`, `deleted`, `could it have been`, `Error:`. Add new frontend
  error strings here first (`test-dom-scan-guard.sh` asserts them, and asserts the
  regex does not fire on clean tile text).

The gate scans **every** published dashboard and never skips one: a dashboard the
product no longer stands behind has to be retired by `install.sh` (see
`superset/dashboards/retired_dashboards.txt`), not hidden from the gate — a quiet
skip is exactly the false pass this script exists to prevent.

Requirements: node >= 22 (global `WebSocket`) and a Chromium binary on the box
that runs it. Env knobs: `SUPERSET_PORT` (8088), `SUPERSET_USER`/`SUPERSET_PASSWORD`
(admin/admin, the seed chain's documented default — the session cookie is injected
with `Network.setCookie`, no credential is ever typed into a page),
`CHROMIUM_BIN`, `CDP_PORT` (auto: a free port is chosen, so parallel scans cannot
collide), `DOM_SCAN_OUT`, `DOM_SCAN_HOST`, `LOG_DIR`.

bash test-dom-scan-guard.sh <test-slot>   # proves the false-pass guard: an
                                          # unresolvable target and a wrong
                                          # dashboard id both exit 2

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

## Phase 6 drains the scheduler's catch-up run (t_67bb60ef)

Unpausing `grocery_complete_pipeline` on a freshly reseeded metadata db makes the
scheduler create its **catch-up run** for the most recent `0 */6 * * *` slot (the
slot is already in the past), and the DAG is `max_active_runs=1` — so the cycle's
own run queues behind it. That wait is not harmless: the catch-up run does the
same ingest+dbt work, and `grocery_dbt` carries `retries=1 / retry_delay=2m`, so
one of its failed dbt tasks **comes back** while the cycle's own run is in its
transform. Two `grocery_dbt` runs in flight drop/recreate staging relations under
each other: on 2026-09-21 20:58:13 UTC a cycle's `transform` died on
`relation "staging.stg_pos_transaction_items" does not exist`, 32 s after that
same cycle's staging task had created it — a verdict about the scheduler, not
about the code under test.

So phase 6 waits for the slot to come to rest **before** triggering its own run:

```
  DRAIN: no tracked run in flight (0s) — confirming a clear slot for 90s
  DRAIN: waiting on grocery_complete_pipeline scheduled__2026-09-21T18:00:00+00:00 running (95s)
  DRAIN: waiting on grocery_dbt manual__2026-09-21T21:22:29.507838+00:00 running (110s)
  DRAIN: slot clear after 425s — drained run_id(s): scheduled__2026-09-21T18:00:00+00:00 manual__2026-09-21T21:22:29.507838+00:00
  DRAIN: how the drained run(s) ended:
         grocery_complete_pipeline scheduled__2026-09-21T18:00:00+00:00 failed end=…
         grocery_dbt manual__2026-09-21T21:22:29.507838+00:00 failed end=…
```

- **Observable by design.** Every run it waited on is printed with the state it
  was seen in, and the summary names the `run_id`s and the state each of them
  *ended* in — a drained slot and a clean slot read differently (`… nothing was
  in flight (clean slot, no run drained)`), and a drained run is shown to have
  settled (`success`/`failed`), not to have vanished.
- **The clear verdict has to survive `DRAIN_SETTLE_S` (default 90s).** The
  scheduler creates the catch-up run seconds after the unpause, so a single empty
  reading would race exactly the run the drain exists to avoid.
- **Never at rest = FAIL**, not a queued cycle: after `DRAIN_MAX_MIN` (default 20,
  the catch-up run takes ~7 min) the phase fails with the runs it was still
  waiting on. An unreadable metadata db is not an empty slot either — the drain
  refuses (same contract as the at-rest guard).
- **While the cycle's run is in flight**, `wait_dag_run` keeps watching for a run
  the cycle did *not* start — a scheduler-created run (a schedule boundary
  crossing, or a catch-up run that reappeared despite the drain) or a hand
  trigger, printed as `RACE: <dag_id> <run_id> <state> (<run_type>) is in flight
  while …`, and summarised as a `WARN` at the end of phase 6. A boundary crossing
  is a scheduler event, not a data-lab defect, so it warns rather than failing —
  but the run_ids are in the log, so a later dbt failure is not left unexplained.
  The cycle's **own** children (the `grocery_ingest_api` / `grocery_dbt` runs its
  pipeline run starts) are excluded by `dag_run.run_type`: a run a pipeline task
  started is `operator_triggered`, a timetable or REST-triggered one is not. That
  filter is load-bearing — the first live run of this drain matched on `dag_run`
  rows alone and reported its own `grocery_dbt` child as a competing run
  (`22:41:14`), i.e. a `WARN` on every healthy cycle.
- Knobs: `DRAIN_POLL_S` (15), `DRAIN_SETTLE_S` (90 — must exceed unpause →
  catch-up-created latency), `DRAIN_MAX_MIN` (20).

`bash test-full-cycle-drain.sh` covers all of the above offline.

## Which slot to run this on

`full-cycle.sh --verify` produces a verdict, so it belongs on the **test** slot
(CT107, <test-slot>): a verification loop must not share a host with the
development loop (dev, CT106 / <dev-slot>).

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

## The dbt view swap — `test-staging-view-swap.sh` (t_b48af51f)

Phase 4/5 can fail on a defect that is entirely inside one dbt run, so it is worth
being able to reproduce without a cycle:

```bash
bash e2e-testing/test-staging-view-swap.sh
```

dbt-postgres rebuilds a view as `rename to x__dbt_backup` → `create view x` →
`drop view x__dbt_backup cascade`. A view that reads `x` follows the rename onto
the backup, so the CASCADE deletes it: rebuilding `stg_pos_products` after
`stg_pos_transaction_items` — which is what `grocery_dbt`'s old per-model parallel
staging tasks could do at any time, ~50/50 per cycle — leaves the dependent
MISSING with nothing left to rebuild it. `transform` then fails with
`relation "staging.stg_pos_transaction_items" does not exist`, on retries too (the
`grocery_dbt` retries=1 cannot recover a task that no longer has a builder).

The suite reproduces exactly that against a **scratch schema** in the local EDW
(`PROBE_SCHEMA`, unique per run, dropped at the end; the deployed `staging` schema
is never written to): separate invocations rebuild the dependency and the
dependent is gone, then one `dbt run --select staging` and a second one on the
swap path with every staging relation still resolving and dbt's own
`run_results.json` showing the dependency completed first. 20 assertions; run it
on either slot (`--project-src` points it at a candidate tree, `--keep` leaves the
schema behind). It needs a loaded raw layer.

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
