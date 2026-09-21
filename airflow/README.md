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

### Recovering from an excess (what to do when the invariant fires)

The invariant *detects*; it cannot repair. An excess means the raw layer was filled
from the wrong source (see "Source Addressing"), and unlike a shortfall it cannot be
fixed by loading more rows — the correct data and the foreign data are in the same
tables, and the incremental tables only fetch rows newer than their watermark, so the
foreign history would sit there forever.

The raw layer is derived data: drop it and let the ingest rebuild it from the source.

```bash
# 0. NOT while anything is loading. Dropping the raw layer is a write: it is
#    outside the per-table ingest lock, so it can only be safe when no load is
#    in flight. If this returns rows, wait for them (or stop them) first.
docker exec postgres psql -U postgres -d grocery -c \
  "select dag_id, run_id, state from dag_run where state in ('queued','running')"

# 1. read the state first — this is what you are about to throw away
docker exec postgres psql -U postgres -d grocery -c \
  "select schemaname, sum(n_live_tup) from pg_stat_user_tables group by 1 order by 1"

# 2. drop every raw schema (they are recreated lazily by _ensure_table)
docker exec postgres psql -U postgres -d grocery -tAc \
  "select format('drop schema %I cascade;', nspname) from pg_namespace where nspname like 'raw\\_%'" \
  | docker exec -i postgres psql -U postgres -d grocery

# 3. one fresh run rebuilds raw → staging → marts from scratch
docker exec airflow-apiserver airflow dags unpause grocery_complete_pipeline
```

Step 3 must be **run to completion** before anything reads the platform. Between
step 2 and the end of the rebuild the raw layer is legitimately empty and no run
is in flight. An at-rest guard that only looks at in-flight `dag_run` rows passes
in that window, and on 2026-09-21 a `full-cycle.sh --verify` landed exactly in it
and reported the platform as broken — "raw empty-ish (0)", "only 0 of 0 populated
marts" and four unreadable parities, 32s after the failed load ended and 40s
before the rebuild started (t_657cebc3, t_77ac6468).

`full-cycle.sh` now treats that state as a refusal, not a finding: when the raw
layer is empty (or the marts are) and the last *ended* `grocery_ingest_api` /
`grocery_complete_pipeline` run is not `success`, `--verify` exits 3 with "raw
layer mid-rebuild / last load did not succeed — cannot verify" and prints the
re-run commands above, instead of a FAIL list about rows that are missing because
the platform is mid-reload. The distinction is the run state, not the emptiness:
an empty layer behind a successful load is still a data failure (the raw layer
went missing after it was loaded), and so are empty marts behind a successful
load when the transform never ran.

## One writer on the raw layer

`grocery_ingest_api` TRUNCATEs (or drops) each raw table before it refills it, so
two loaders on one table destroy each other. Two guards, because either alone has
a hole:

- `max_active_runs=1` on `grocery_ingest_api` and on `grocery_complete_pipeline`
  (which triggers it) keeps two DagRuns of the same DAG on one scheduler apart.
  It cannot see a second Airflow, a hand-run loader, `airflow tasks test`, or an
  out-of-band DDL.
- A per-table, session-scoped advisory lock in `ingest_table`
  (`INGEST_LOCK_NAMESPACE`, wait bounded by `INGEST_LOCK_WAIT_S`, default 900 s)
  serialises every writer that goes through the database, from any host. A second
  loader waits and then fails naming the table and the holder; a killed task
  cannot leave a table locked. Different tables do not block each other.

So: never add a path that loads a raw table without going through `ingest_table`
(or taking the same lock), and never drop raw schemas while a load is in flight.
`dags/tests/test_ingest_serialization.py` enforces both the configuration and the
lock behaviour:

```bash
docker exec airflow-worker python /opt/airflow/dags/tests/test_ingest_serialization.py
```

On 2026-09-21 this was the last step between the dev slot and a green run: the raw
layer held 1,136,360 foreign `transactions` (local source: 98,477) and 1,695,504
foreign `transaction_items` (local: 571,932), plus 49x `transport.loads` — the
invariant fired on 17 of 27 readable relations and the pipeline could not pass until
the layer was dropped (t_7c88f2f9).

The staging and `mart*` schemas are rebuilt by `grocery_dbt` too; if an excess was
ever *aggregated* into a mart, drop those schemas as well rather than trusting an
incremental re-run of the transform.

## Incremental loads, and forcing a full reload

`TABLE_CONFIGS` gives every table one of two strategies. `full` TRUNCATEs the raw
table and re-reads everything the source holds; `incremental` fetches the window
`[MAX(watermark_col) in raw, now]` — widened by a per-table lookback where the
watermark column is a *backdating* column (`INCREMENTAL_LOOKBACK_DAYS`, empty today;
see the next section) — and upserts it.

The trade-off, stated because it is not free: **a full reload heals any past gap on
every run, an incremental load does not — a delta that was missed stays missed**
until someone forces the whole history back in. That is affordable only where the
watermark column is monotone in insert order and immutable afterwards, and each
switch is justified against the source's own write pattern in a comment on its
`TABLE_CONFIGS` entry (read the generator, don't assume).

### Backdated date columns need the insert clock (t_b474c79e, t_886f7d67)

Six tables used to watermark on a date column the source's generator **backdates**,
which is the one property a watermark column must not have: a batch is INSERTed at
tick time while most of its rows are dated in the past, so a `[MAX(<date column>),
now]` window can never see the rows a later batch backdates below the watermark.
The loss is permanent and silent — the run reconciliation only knows the *requested
window's* advertised total (a row that never entered a window was never advertised),
and `verify_raw_vs_source` fails on excess only.

| table | date column it used to watermark on | why that column is backdated |
|---|---|---|
| `pos_returns`, `pos_return_items` | `return_dt` | `return_dt = txn_dt + randint(2, 21)` days, clamped to the tick, for transactions aged 2-14 days |
| `pos_transactions`, `pos_transaction_items` | `transaction_dt` | the backfill replays a day hour by hour (`main.py` calls `pos.generate_pos_transactions(..., sim_dt=hour boundary)`), so a whole hour's batch is the hour it belongs to, not the moment it was written |
| `online_orders`, `online_order_items` | `placed_dt` | same replay: `online.generate_online_orders(..., sim_dt)` writes `placed_dt = sim_dt` |

All six now take `created_after` / `created_before` on the source and watermark on
`created_at` — the insert clock, `DEFAULT NOW()`, written by the same statement as
the row, so it is monotone in insert order and immutable. The two item routes have
no timestamp of their own and join the header's clock (`pos.transactions.created_at`,
`online.orders.created_at`); `online.order_items` has no `created_at` column of its
own either, which is why the routes gained it in the payload rather than in the
table. `start_dt`/`end_dt` still filter the date columns on every one of these
routes, unchanged in meaning.

Measured cases, both on the dev slot:

- the source's `2026-09-21T07:08:27Z` returns batch (80 returns / 109 lines, whose
  `return_dt` reached back to 2026-08-24): the old `[MAX(return_dt), now]` window
  reached **53/80 returns and 74/109 lines**; the `created_at` window **80/80 and
  109/109**, with `total` matching SQL exactly.
- a POS gap-fill (the backfill replaying the early hours of 2026-09-21): 10
  transactions — `transaction_dt` 00:00/01:00/02:00, `created_at` 03:19:17 and
  03:50:34 — sat at-or-below raw's own `MAX(transaction_dt)`
  (`2026-09-21T03:24:18.390145-04`) out of 95031 source rows against raw's 95009,
  with 129 of the 550069 transaction lines. Those rows were **already** below the
  watermark, i.e. beyond any `created_at` window that starts at raw's own
  `MAX(created_at)` as well — the insert clock prices the *delta*, it does not
  reach backwards. They were healed with a one-off `full` run on the two entries
  before the switch (`full` TRUNCATEs and re-reads the relation, so the result is
  an exact mirror), verified key-for-key: 95031 of 95031 transactions, md5 over
  the sorted key list identical on both sides, 0 missing / 0 stale. One of the 10
  (`837b2ab3-8ccd-4ebc-88ad-cf93dfed8ce2`) was the live dbt WARN
  `relationships_stg_pos_loyalty_point_transactions...` — the loyalty-point row is
  on the insert clock and had loaded fine; the transaction it references had not.

Three things to know before copying this shape onto another table:

- **`created_at` is the watermark, not the only window.** `start_dt`/`end_dt` still
  bound the date columns on these routes. A *params-driven* run (the full-reload
  recipe below) reads the hardcoded `params_conf["start_dt"]/["end_dt"]` keys and
  applies those values to `created_after`/`created_before` now — harmless for
  `2000-01-01 → 2100-01-01`, which covers every row that exists, but no longer a
  `return_dt`/`transaction_dt` window. A narrow historical window on the date
  column is a `full`-strategy load, not a params run of these entries.
- **Adopting a new watermark column takes two runs.** The column reaches the raw
  table only with the load that carries it (`_detect_schema_drift` auto-ALTERs a new
  payload column in as TEXT), so the *first* load has no watermark to read:
  `ingest_table` falls back to the last `INCREMENTAL_FALLBACK_DAYS` (365) days and
  logs a WARNING naming the column. That is complete only if the table's whole
  history sits inside the horizon — true for these six (every row of all of them is
  an insert of the current day on the dev dataset), and verified key-for-key against
  the source rather than by row count, because neither the run's reconciliation nor
  `verify_raw_vs_source` can see a row that never entered the window. A table whose
  history is older than the horizon owes the param-driven full reload below. The
  transition is per entry: `pos_return_items` took it in t_b474c79e, the online pair
  took it in t_886f7d67, and the POS pair did *not*, because the gap heal that
  preceded their switch was itself a load of the new payload and delivered the
  column.
- **A gap that is already below the watermark is not healed by the switch.** The
  insert clock makes the delta exact from the moment it is adopted; it cannot reach
  rows that landed under an older watermark. Measure key-for-key against the source
  before switching (the run may take longer than one ingest cycle), and heal with the
  recipe below or a one-off `full` run on that entry.

The bounded reach-back that `pos_returns` and `pos_return_items` used between
t_4788529f and t_b474c79e (21 days of `return_dt` — the generator's own maximum
offset) is still implemented and tested, with **no table registered**: it is the
shape a backdating watermark needs, and every table that needed it is now on its
route's insert clock. Register a table there rather than inventing a second
mechanism if a route with a backdating watermark turns up without one.

`dags/tests/test_incremental_watermarks.py` pins all of it: all six tables on the
insert clock with the right bound names, no configured lookback while the mechanism
still shifts a window when one is registered, the transition fallback (with its
WARNING), the param window landing on the `created_at` bounds, and — against the
running source — that all six routes really declare `created_after`/`created_before`
*and* really filter on them (FastAPI ignores an unknown query parameter *silently*,
so a config naming a bound the route does not implement reads as a windowed fetch
while returning the whole table — the failure mode `online_order_items` spent a run
on; the filter check compares the route's `total` for a one-batch window against the
source's own SQL count for it):

```bash
docker exec airflow-worker python /opt/airflow/dags/tests/test_incremental_watermarks.py
```

Two guards stay meaningful on a partial raw table, and neither of them makes the
paragraph above untrue:

- the run's own reconciliation fails a task whose *requested window* advertised
  more rows than landed (it cannot see a row that never entered the window);
- `verify_raw_vs_source` is one-sided on purpose — it fails on excess, so a source
  reseeded under a populated raw table is still loud, while a partial raw passes by
  construction. Nothing asserts that `raw_online.order_items` is a full mirror:
  `stg_online_order_items` and its `relationships_*` test only check uniqueness,
  not-null and FK direction (`assert_e2e_row_count_propagation` does not list it).

Force the whole history back into every incremental table in one run — upsert, no
TRUNCATE, so it heals missing rows without dropping rows the source no longer has:

```bash
docker exec airflow-apiserver airflow dags trigger grocery_ingest_api \
  --conf '{"start_dt": "2000-01-01T00:00:00", "end_dt": "2100-01-01T00:00:00"}'
```

The DAG params replace the watermark window for **every** incremental table in that
run (measured 2026-09-21: all 11 showed `param window: 2000-01-01T00:00:00 →
2100-01-01T00:00:00`), so budget for it. A 100-year window is bisected into pieces
rather than paged, and the source is asked for the whole table: the run took
**117.7 s and 1691 HTTP requests** end to end (order-items alone 265 requests /
36.9 s for 210114 rows, against 1 request / 0.4 s for its delta — and 45 for a paged
full load). It is a recovery, not a routine. To rebuild a single table as a
truncating full load instead — the only way to drop rows the source no longer has —
put `"full"` back on that entry, or drop just its raw table and run with the params
above (an emptied table alone is not enough: the incremental fallback is the last
365 days).

