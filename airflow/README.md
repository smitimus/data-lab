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

## Read order: a referencing route is read before the route it references

`grocery_ingest_api` runs one task per route, four at a time, so **the order the
routes are read in is a property of the raw layer**, not an implementation
detail. Every read is a point-in-time snapshot of a live source, and a row can
only carry a foreign key whose parent was already committed when that route was
read. Read a child route *after* its parent and it can pick up a generator tick
the parent's window had already closed over: the events land, the orders never
do, and the staging `relationships` test fails the pipeline on a healthy ingest.

`REFERENCING_ROUTES` in `dags/grocery_ingest_api.py` declares that order as task
dependencies — the child finishes reading, then the route it references starts.
The list is exactly the cross-route `relationships` tests in
`dbt/grocery/models/staging/staging.yml` (master data included), and
`dags/tests/test_load_order.py` fails when the two drift apart, so a new FK test
cannot pass without its read order being declared. Airflow dependencies are
transitive: `pos_return_items → pos_transaction_items → pos_transactions` needs
only the two adjacent edges.

Reading the child first is what makes the FK hold, and it holds in one direction
only: the parent's window then ends *after* everything the child could have
seen, and its start is either `MAX(watermark)` in raw — above which every row it
has not loaded necessarily sits — or, for a `full` reload, a fresh mirror of the
source. The opposite skew (a parent whose child has not arrived yet) is the
benign direction: the next run delivers the child, and nothing asserts on it.

### The read *cut*: the parent's window ends where its children's reads ended

Ending "after everything the child could have seen" is not enough on its own. It
leaves the parent's window bounded by its own task start, so the foreign key holds
by an accident of scheduling — the parent happened to start later — and nothing in
the DAG says it has to. It also reads *past* what its children saw, which is the
wrong side for the reverse invariant on the same pair:
`assert_online_orders_reconcile` asserts that a completed order has its final
lifecycle event.

So each route returns the instant its own read ended — its window end for an
incremental load (rows newer than that are filtered out even though they were
physically read), its last read for a whole-table one — and a referenced route
takes the **latest** of its referencing routes' reads as the end of its own window
(`cut_from` in `ingest_table`, wired from the same edge list). Latest, not
earliest: the earliest child's end leaves the events route — now a windowed load
but a `full` one until t_5a16129f, so the last to finish reading — reading past
the orders window, measured as 29 orphan events on the dev slot. With the latest,
a referenced route covers everything that referenced it, so every event's parent
is inside the parent's window and an order's state is read no later than the
events route read. A route nothing references keeps its own task start, unchanged,
and a `full` route ignores the cut — it reads the whole table, so it already
covers any cut its children can report.

Two exceptions, both needed by the same route:

* `online_orders` is an **as-of** route (`AS_OF_ROUTES`), because its rows mutate
  after insert — see the next section. It takes the cut of the one route that
  shares its clock when the row changes (`online_order_events`), not the latest of
  its children's cuts.
* `online_order_events` is **incremental** since t_5a16129f (it used to be the
  full reload in the table below), which is what makes the pair's shared instant
  an instant rather than "the last moment the events route happened to read".

Measured on the dev slot, 2026-09-21 (30 s generator ticks), before the order
existed:

| route | read window | note |
|-------|-------------|------|
| `online_orders` | 14:00:05.98 → 14:00:10.27 | incremental; window ends at its own task start |
| `online_order_events` | 14:00:09.72 → 14:00:28.90 | **full** reload; no window, pages past its own start |

The events route started 3.7 s after the orders route and reloaded 145k rows over
19 s, so it read the 14:00:13 and 14:00:27 ticks that the orders window — closed
at 14:00:05.98 — could no longer see. 8 staged events with no staged order (10 at
20:04:13.717 on the test slot's fresh seed, which is how t_7e427ee6 was filed).
A tick landing between two reads is not rare: the interval is 30 s and a full
reload of that route takes ~19 s.

To check the whole FK set by hand (staging vs staging, no source needed):

```sql
select count(*) as orphan_events from staging.stg_online_order_events e
 where not exists (select 1 from staging.stg_online_orders o where o.order_id = e.order_id);
```

### The as-of bound: a route whose rows mutate (t_5a16129f)

One pair needs a third thing on top of the read order and the cut, because its rows
**mutate after they are inserted**. An online order's `status` moves long after its
`created_at` (placed → confirmed → picking → ready → completed, each with an event),
so there are two clocks and the insert clock can carry only one of them:

* it cannot see a state change. A window anchored on `created_at` never re-reads a
  row the watermark has passed, so an order loaded as `placed` keeps that status in
  raw forever. Nothing asserted on it — the reconcile test looks for completed
  orders, not stale ones — and the events route being a FULL reload hid it, because
  the pair was as fresh as the last full read of it.
* it cannot be bounded safely. **This is the completion residual t_7e427ee6 filed
  and this card closes.** With the window ending at the events route's read instant
  but anchored on `created_at`, an order that completes *while the orders route is
  being read* is still inside the window (it was created long before it completed)
  and lands as `status='completed'` while its terminal event, inserted after that
  instant, is not read until the next run. `assert_online_orders_reconcile` catches
  exactly that. On the test slot's 2026-09-21T21:22:29Z fresh seed it failed with 5
  rows while the FK test passed; the next events read healed it, which is what made
  it look like a flake. The window grows with the read, so the residual does too:
  worst on a fresh seed (a 30-day window), invisible on a narrow delta.

**The fix.** All three routes of the cluster are windowed now and all three windows
end at the *same instant* — the instant `online_order_events` read to:

| route | window | end bound |
|-------|--------|-----------|
| `online_order_events` | `[MAX(created_at) in raw, its own task start]` | the `cut` it hands to the other two |
| `online_order_items` | `[MAX(created_at) in raw, that same instant]` | **snapshot-bound** (`SNAPSHOT_BOUND_ROUTES`): not its own task start |
| `online_orders` | `[MAX(updated_at) in raw, that same instant]` | **as-of** (`AS_OF_ROUTES`): not its own task start, and not the latest of its children's cuts |

The items row is not decoration. Bounding only the orders route closed the
completion residual but left the other edge of the same cluster accidental: the
items route ended at its own task start, seconds *later*, and an order inserted in
that gap — the same transaction inserts the item that references it — is inside the
items window and outside the orders window. The item loads, its order does not, and
no later run re-reads it (an insert clock does not look back). It held on the dev
slot by accident of task creation order, which is the "by accident of scheduling"
t_7e427ee6's own comment rejects. Declaring it makes the read order
`events → items → orders` and the instant shared by construction. Delaying an item
past the instant is safe where delaying a state change is not: `created_at` is
immutable and monotone, so the item is read by the next run — its insert clock is
above the next watermark.

`online_order_events` is incremental since t_5a16129f (it used to be the full
reload above): the source route gained `created_after`/`created_before` on
`online.order_events.created_at` and returns the column (verisim `e9bd295`, card
t_51bbc12e). That is what makes the pair's shared instant an instant — a `full`
route ignores the cut and reports its last read instead.

`online_orders` is declared in `AS_OF_ROUTES`: it is read as of the instant
`online_order_events` read to, on an end bound named for the clock that moves when
the row does (`updated_before`). The clock is the child's own for the same row —
the generator writes a status change and the event that explains it in **one
transaction**, so `updated_at == MAX(order_events.created_at)` per order (0
differing of 39243 on the dev slot 2026-09-21; verisim `80b3e0f` guards it
source-side). That equality is what makes the bound safe: no order is loaded ahead
of the event that explains it.

The lower bound has to move with the upper one. Excluding a row for having moved
after the bound while water-marking on `created_at` would make it *lost*, not
delayed: its `created_at` is below the next insert-clock watermark, so no later run
would reach it. On the state clock it is delayed by exactly one run.

**Measured on the dev slot, 2026-09-21** (source read-only probe, at the instant a
run's pair ended at — `manual_t_5a16129f_r2`, 22:32:04Z):

| reader | order-loads as `completed` whose terminal event is stamped after the instant |
|---|---|
| created-clock window (before) | **77** at that instant; **260475** summed over 155 sampled read ends (30-day window) |
| state-bounded window (after) | **0** at that instant; **0** over the same 155 |

and the same run's windows, from the task logs:

| run | `online_order_events` | `online_order_items` | `online_orders` |
|---|---|---|---|
| first run after the change (raw has no `created_at`/`updated_at` yet → the transition fallback, WARNING, column ALTERed in) | 188249 rows, window `now-365d → 22:30:40.726142Z` | — (that run predates the items bound) | 38740 rows, window `now-365d → 22:30:40.726142Z` (its end = the events route's instant) |
| next run (steady state) | **828 rows**, `18:30:10-04:00 → 22:32:04.405018Z` | — | **545 rows**, `18:30:10-04:00 → 22:32:04.405018Z` |
| with the items bound in place (`_r3`) | 10114 rows, `18:31:41-04:00 → 22:55:55.217477Z` | 44682 rows, `18:31:41-04:00 → 22:55:55.217477Z` | 4457 rows, `18:31:41-04:00 → 22:55:55.217477Z` |
| the run after that (`_r4`) | 361 rows, `18:55:51-04:00 → 22:56:40.620225Z` | 1723 rows, `18:55:51-04:00 → 22:56:40.620225Z` | 258 rows, `18:55:51-04:00 → 22:56:40.620225Z` |

Every run reconciled exactly (distinct keys landed == the source's advertised
total), all three windows of a run end at the same instant to the microsecond, and
afterwards `raw_online` had 0 completed orders without a final event, 0 items whose
order is missing and 0 events whose order is missing (staging agrees — that is the
`dbt` layer's FK and reconcile tests, by hand).

Two guards worth knowing about:

* an as-of route — and a snapshot-bound one — **refuses to load** when the route
  whose instant it ends at returned no read cut, instead of falling back to its own
  task start. The fallback is what the residual was, and what the orphan-item hole
  was; a run that cannot bound the rows it loads is a failed run, not a degraded one.
* `dags/tests/test_load_order.py` 5a–5f/6a–6d and
  `dags/tests/test_incremental_watermarks.py` 6a–6f fail if the declaration,
  `TABLE_CONFIGS`, the read order and the DAG wiring drift apart — including a
  provider that stops being windowed, which would silently end the cluster at two
  different instants again.

To check the residual by hand at any instant B (source, read-only):

```sql
-- orders that completed AFTER B, as the reader bounded at B sees them
select o.order_id from online.orders o
 where o.updated_at <= '2026-09-21T22:32:04.405018+00:00'::timestamptz
   and o.status = 'completed'
   and (select max(e.created_at) from online.order_events e
         where e.order_id = o.order_id
           and e.event_type in ('picked_up', 'delivered'))
       > '2026-09-21T22:32:04.405018+00:00'::timestamptz;
-- 0 rows: every completed order in the window has its terminal event inside it.
-- Swap `updated_at` for `created_at` above and the same query returns rows —
-- that is the residual, and it is what this pair used to load.
```

The other edge of the cluster, checked the same way — an item loaded against an
order the same instant's window did not carry:

```sql
-- raw items whose order is absent, after any run
select count(*) from raw_online.order_items i
 where not exists (select 1 from raw_online.orders o where o.order_id = i.order_id);
-- 0: the order the item references is inside the orders window, because the item
-- is inside the events window that both the orders window and this one end at.
```

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

All six took `created_after` / `created_before` on the source and watermark on
`created_at` — the insert clock, `DEFAULT NOW()`, written by the same statement as
the row, so it is monotone in insert order and immutable. The two item routes have
no timestamp of their own and join the header's clock (`pos.transactions.created_at`,
`online.orders.created_at`); `online.order_items` has no `created_at` column of its
own either, which is why the routes gained it in the payload rather than in the
table. `start_dt`/`end_dt` still filter the date columns on every one of these
routes, unchanged in meaning.

**One of the six has since moved again, for a different reason:** `online_orders`
is on the *state* clock (`updated_at`) since t_5a16129f, because its `status` moves
long after the row is written and a pair whose two sides are judged at one instant
cannot be bounded on the insert clock. That is a different defect from the
backdating one above; see "The as-of bound" earlier in this file. `online_order_items`
stays on the insert clock — a line has no state to be as-of.

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

## One invocation per dbt layer: a view swap cascades (t_b48af51f)

dbt-postgres rebuilds a **view** in three statements:

```sql
alter view staging.stg_pos_products rename to stg_pos_products__dbt_backup;
create view staging.stg_pos_products as …;
drop view staging.stg_pos_products__dbt_backup cascade;   -- dependents die here
```

A view that reads `stg_pos_products` is not re-pointed by the rename — PostgreSQL
binds the dependency by OID, so the dependent **follows the rename onto the
backup** — and the trailing `CASCADE` then deletes it. `staging.
stg_pos_transaction_items` is a view over `stg_pos_products`
(`dbt/grocery/models/staging/stg_pos_transaction_items.sql`), so any run in which
`stg_pos_products` is rebuilt *after* its dependent, with nothing rebuilding the
dependent afterwards, leaves the dependent MISSING: its 14 tests and the two
intermediate models that read it fail on `relation "staging.
stg_pos_transaction_items" does not exist`, and the DAG's `retries=1` cannot
recover because no task re-creates the view (CT107, 2026-09-21, `transform`
failed on two consecutive cycles).

`grocery_dbt` used to build staging as **one Airflow task per model**, all in
parallel inside a single dag_run — an ordering no Airflow edge expressed and dbt
never got to enforce, so which of the two finished second was a coin flip per
cycle. It now runs the layer as **one** `dbt run --select staging`
(`airflow/dags/grocery_dbt.py`, `staging.run_staging`), which hands the ordering
back to dbt: the swap for a dependency always completes before its dependent is
rebuilt in the same run. The cost is per-model Airflow visibility/retries in that
layer; marts and intermediate tables are unaffected (a table's dependents do not
ride along on its swap, and nothing in this project reads `mart*` from a view).

Two guards:

* `dags/tests/test_dbt_staging_order.py` reads the staging SQL and the DAG (no
  database) and fails when a staging model `ref`s another and the build order is
  not established — either by the single layer invocation or, if the layer is ever
  split again, by an Airflow path from the dependency's task to the dependent's.
  It also fails when a *new* intra-staging `ref` appears, so the next author has to
  come and declare it rather than discovering the hazard in a failed cycle:

  ```bash
  docker exec airflow-worker python /opt/airflow/dags/tests/test_dbt_staging_order.py
  ```

* `e2e-testing/test-staging-view-swap.sh` runs the real project against a scratch
  schema and asserts the whole thing end to end: the defect (rebuild the
  dependency in a separate invocation and the dependent IS gone), then one layer
  invocation and a second one on the swap path with every staging relation still
  resolving and dbt's own `run_results.json` showing the dependency completed
  first. It drops its schema and never writes to the deployed `staging`:

  ```bash
  bash e2e-testing/test-staging-view-swap.sh                    # deployed project
  bash e2e-testing/test-staging-view-swap.sh --project-src /path/to/candidate/dbt/grocery
  ```

  Measured on the dev slot (CT106) 2026-09-21: PASS, 20 assertions; the layer runs
  32 view models in ~1.8 s, and step 1 reproduces the loss with dbt's own
  `Applying DROP to: …stg_pos_products__dbt_backup` in the log.

