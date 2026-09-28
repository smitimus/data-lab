# Apache Superset

## Access
| Item     | Value                              |
|----------|------------------------------------|
| URL      | http://YOUR_SERVER_IP:8088         |
| Username | admin                              |
| Password | admin                              |
| Port     | 8088                               |

## What It Does
BI and dashboarding platform. Pre-loaded with dashboards built on the verisim grocery data marts. Connected to the `edw` PostgreSQL database.

## Key Config Files
- `_conf/superset/superset_config.py` — Superset configuration (seeded from `stacks/superset/superset_config.py` by `init.sh`)
- `stacks/superset/dashboards/` — Exported dashboard JSON files for import

## Usage Notes
- **Dashboard import:** `install.sh` imports `superset/dashboards/*.zip` in the background once Superset is healthy — but only after the marts exist, because each bundled dataset needs its `mart` table. On a fresh install it defers (the first pipeline run has not happened yet); re-run it once the DAG has populated the marts with `bash /opt/data-lab/install.sh --dashboards-only`, which imports idempotently (`overwrite=true`) and reports per object. Manual import: Dashboards → ⋮ → Import → select the zip.
- **The bundle is a contract with the importer** (`superset/dashboards/`):
  - A dashboard's layout only survives an import into another instance because
    every CHART node carries `meta.uuid`. Superset's importer rebinds
    `meta.chartId` (a per-instance slice id) from that uuid — `update_id_refs()`
    in `dashboard/importers/v1/utils.py` — and `find_chart_uuids(position)`
    decides which charts the dashboard needs and which ones get linked to it
    (`dashboard_slices`). A node without a uuid keeps the *exporting* instance's
    id, and a chart no node names is never imported: that is a tile saying
    "There is no chart definition associated with this component".
  - `bind_dashboard_layout.py` is the offline repair for that: it binds every
    CHART node to the uuid of the chart it names (through the `_<id>.yaml` suffix
    of the bundled chart file), refuses a node whose chart the bundle does not
    ship, drops a chart that appears twice on one dashboard (legacy layout next
    to the current one), and drops dashboards listed in `retired_dashboards.txt`
    — and then prunes the archive to the CLOSURE of the dashboards it still
    ships: a chart no shipped layout names, a dataset no shipped chart points at
    and a database no shipped dataset points at are dropped too, because the
    importer never imports them while `check_zip_landed()` demands that every
    bundled object landed (retiring a dashboard without its charts turns
    `--dashboards-only` red with "only 10/18 charts present"). `--check` asserts
    all of it without writing — bindings, no orphans, no retired dashboard. Run
    it after re-exporting. Asserted offline by
    `bash e2e-testing/test-bind-dashboard-layout.sh`.
  - `repair_query_context.py` is the sibling repair: every chart must carry a
    `query_context` or the tile renders "Chart has no query context saved".
  - `normalise_chart_metrics.py` is the third repair, for the *metric key of the
    chart family that reads it*: a pie / big_number / big_number_total chart
    reads the singular `params.metric`, and one carrying only the plural
    `metrics` sends `orderby: [[null, false]]` — Superset answers 400 "Field may
    not be null" and the tile renders "Unexpected error" forever *while its
    query_context, every API call and every DB gate stay green* (proved live
    2026-09-21). The rule itself lives in `superset/_superset_chart_params.py`
    and is applied by `create_grocery_ops_dashboard.py` to everything it seeds,
    so the seed and this repair cannot drift; `--check` is the dev-side
    assertion and `e2e-testing/lib/bundle_metrics.py` (run from
    `bash e2e-testing/test-install-dashboards.sh`) asserts the shipped bundle.
    Run all three repairs after re-exporting a bundle.
  - `retired_dashboards.txt` lists dashboards an older bundle shipped and the
    current one does not. The import is additive (`overwrite=true` re-creates and
    updates, never deletes), so `install.sh`'s `retire_dashboards()` deletes them
    by uuid on every import — that is how an instance that imported an older
    bundle converges on the current one.
  - **A dashboard the bundle ships must carry a `slug`.** Superset's importer
    resolves an imported dashboard by uuid first and, when that misses, by the
    model's unique constraint — for `dashboards` that is `slug`
    (`commands/dashboard/importers/v1/utils.py`, then `models/helpers.py`
    `ImportFromDictMixin.import_from_dict` via `cls._unique_constraints()`). A
    bundled dashboard with `slug: null` has nothing to match on, so every import
    creates a SECOND dashboard with the same title next to the one the scripted
    seed already built — plus a parallel generation of its charts, since chart
    identity is the uuid. That is how "Grocery Overview" had two owners
    (t_23a97d10, retired 2026-09-21): `Grocery_Operations_5.yaml` ships
    `slug: grocery-operations` and updates the seed's row in place, one row;
    `Grocery_Overview_4.yaml` shipped `slug: null` and produced a 12th dashboard
    where the gates expect 11. The rule of thumb: a dashboard the bundle cannot
    converge onto an existing row belongs in `retired_dashboards.txt`, not in the
    bundle — the scripted seeds own the dashboards they build.
- **...and the links are a contract too** (`install.sh`'s
  `reconcile_dashboard_links()`, right after the import). A dashboard stores its
  charts twice: `position_json` (the tiles) and `dashboard_slices` (the links the
  app hydrates from). The importer only *inserts* links — it never removes the
  ones an earlier import or a `superset-setup` seed run left behind — and a chart
  that is linked but unplaced in the layout is still DRAWN, as an extra tile. So
  after the import `install.sh` makes each bundled dashboard's links equal the
  chart ids its `position_json` names: it unlinks the orphans and re-links a
  layout slot that lost its link. No `slices` row is created or deleted; the
  reconcile is idempotent, so a second `--dashboards-only` reports `0 orphan
  link(s) unlinked, 0 missing link(s) linked`. Asserted offline by
  `bash e2e-testing/test-install-dashboards.sh`.
- **...and the chart ROWS an import displaces are deleted, not left dead**
  (`install.sh`'s `prune_superseded_charts()`, right after the link reconcile).
  A dashboard converges onto an existing row through its `slug`; a CHART has no
  such identity — `commands/chart/importers/v1/utils.py`'s `import_chart()`
  looks the chart up by `uuid` and nothing else — so an export's chart and a
  scripted seed's chart of the same name on the same dataset are two `slices`
  rows that can never merge. On a virgin instance the seed runs first (it is a
  compose service; the import only runs once the marts exist), so the import adds
  its own generation and points the layout at it: Grocery Operations ends up with
  its 10 charts twice (seed ids 9–18, bundle ids 97–106), the seed's rows linked
  to nothing and placed nowhere (measured 2026-09-21, t_a0dc1643).
  **Grocery Operations has one owner, and the bundle's layout decides it.** The
  seed's generation is the FALLBACK a fresh instance renders before the import
  has run — that is what a wipe cycle has (full-cycle.sh runs no import) and what
  its `11+ dashboards` and DOM gates certify — and the import supersedes it on
  every instance that runs the documented install step. What the import displaces
  is then deleted, under three conditions, all of them required: no dashboard's
  `position_json` names it, no dashboard links it in `dashboard_slices` (the
  precedent is `create_data_quality_dashboard.py`'s `_prune_duplicate_chart()`:
  unlink first, and delete only a chart no other dashboard uses — a chart a
  dashboard still links is kept), and a chart a dashboard THIS IMPORT SHIPPED
  places carries the same `slice_name` AND the same `datasource_id`. Scoped to the
  imported dashboards' uuids, so another dashboard's charts are never candidates;
  idempotent, and the DELETE goes through the API so Superset's own relationship
  handling runs. A chart another dashboard still uses is left alone (it renders
  nowhere on the imported one anyway: the layout does not name it). Asserted
  offline by `bash e2e-testing/test-install-dashboards.sh`. The last condition is
  what makes the prune see the WHOLE displaced generation: while
  `create_missing_dashboards.py` adopted a chart by name alone, its dashboard 3
  linked one of the seed's rows (t_0f87aab9), so that row was spared as "linked
  elsewhere" — a chart identity rule, not a prune rule.
- **The seeds own their dashboards, and assert the same link invariant.** A seed
  addresses its own dashboard by SLUG (`grocery-operations`, `grocery-overview`,
  `data-quality-ops`, ...), never by title, and links exactly the charts its
  layout places — `link_charts_to_dashboard()` REPLACES `dashboard.slices`, so a
  title lookup that lands on somebody else's row rewrites that row's links and
  drops it to "There is no chart definition associated with this component"
  tiles (this is what `superset/setup.py` used to do to the bundled Grocery
  Overview, and why the seed now resolves `grocery-overview` and leaves a
  same-titled dashboard it cannot prove is its own completely alone).
  `GROCERY_OVERVIEW_LAYOUT` lists every chart the seed creates, because a chart
  that is linked but unplaced is still drawn as an extra tile.
- **A chart's identity is NAME + DATASET — adopting one by name alone renders the
  wrong mart.** `create_missing_dashboards.py` builds each dashboard from
  `{dataset, name, viz, params}` definitions and REUSES an existing chart instead of
  appending a duplicate on a re-run, so the lookup has to be scoped to the dataset
  the definition resolved. The same title is shipped on different marts by different
  scripts: `Labor Cost % of Revenue` exists on `mart_store_weekly_summary` (the
  Grocery Operations seed AND the bundle), on `mart_employee_cost` (the HR section)
  and on `mart_labor_cost_by_department` (the workforce section), and a name-only
  lookup handed dashboard 3 the store-weekly row — plausible numbers, right title,
  wrong mart, grouped by `location_name` instead of `department`. No gate can see
  that: the chart answers 200, its `query_context` is valid, and the DOM renders a
  chart. `_find_chart_id()` therefore requires `ds_id`, reports a same-named chart
  on another dataset and builds the chart the definition asks for
  (`create_grocery_ops_dashboard.py`'s `_find_chart_ids()` applies the same rule),
  and walks the listing page by page — Superset serves at most 100 rows per request
  whatever `page_size` asks (verified: Flask-AppBuilder's `_sanitize_page_args()`
  clamps to `FAB_API_MAX_PAGE_SIZE`, 100 by default and not overridden here), so a
  single "complete-looking" page never sees a chart past the 100th. Asserted offline
  by `bash e2e-testing/test-seed-chart-identity.sh`.
  **Reuse also RE-ASSERTS the definition.** The payload is built before the existence
  check and the reuse path `PUT`s it over the chart's oldest row (minus `dashboards`,
  which would unlink it), so a chart seeded before `_superset_query_context` existed
  — or imported with `query_context: null`, which renders "Chart has no query context
  saved. Please save the chart again." — heals on the next seed instead of being
  returned untouched (t_fc196131; its `create_missing_dashboards.py` half did not
  survive the CT106 refresh and is re-landed here as t_8d37df71's disposition). A
  refused PUT (e.g. 422) is reported and the row is still reused, so a dashboard
  never silently loses the tile.
  Note the consequence on a chart the seed used to adopt across datasets: it is now
  built on its own mart, so an instance seeded before the fix carries two tiles of
  that title on one dashboard and heals on the next seed + `install.sh
  --dashboards-only` (the reconcile unlinks the orphan, the prune deletes it when a
  placed chart shares its name and dataset).
- **Dataset column metadata is a snapshot — the seeds refresh it.** A dataset's column list (`table_columns`) is read once, at registration. The marts are CTAS-built on every dbt run, so a mart gaining or renaming a column leaves the dataset stale, and a chart that touches a new column then fails in the *browser* while every API and DB gate stays green (the real case: `mart_hourly_sales_pattern` gaining `as_of_date`, which the seeds point `main_dttm_col` at). Every seed therefore refreshes the datasets it binds charts to, through `_superset_dataset_metadata.py`:
  - `setup.py`, `create_missing_dashboards.py`, `create_grocery_ops_dashboard.py` refresh after resolving datasets and before setting `main_dttm_col` / building charts;
  - `create_data_quality_dashboard.py` calls the same helper,
  - so there is one implementation: `refresh_dataset()` / `refresh_datasets()`.
  The seed scripts import that module, so a slot whose `_conf/superset` is updated by hand must get it too (`init.sh` copies it on a reseed). A whole-instance sweep, without re-running a seed:
  ```bash
  docker exec superset python3 /app/superset_home/_superset_dataset_metadata.py --refresh-all
  ```
- **The stale-metadata gate:** `bash superset/verify_dataset_metadata.sh` compares every `mart` dataset's columns against the EDW and exits non-zero when they disagree (0 clean, 1 stale, 2 unreadable). It runs from `verify_seed.sh`, from `full-cycle.sh --verify` (phase 8) and from `install.sh`'s `verify_superset`, and it needs only the postgres container. Its logic is asserted offline by `bash e2e-testing/test-dataset-metadata-gate.sh`.
- **Adding a database connection:** Settings → Database Connections → + Database → PostgreSQL
  - Host: `postgres`, Port: `5432`, Database: `edw`, User: `postgres`, Password: `postgres`
- **First login:** admin / admin → prompted to change password (optional, skip for lab use)
- Data refreshes automatically as the Airflow DAG runs every 15 minutes
