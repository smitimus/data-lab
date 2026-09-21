# AGENTS.md — dbt Grocery Project

dbt project at `airflow/dbt/grocery/`. 27 staging models, 29 mart models, 10 custom tests.

## Structure

```
grocery/
├── dbt_project.yml          # Project config (profile: grocery)
├── packages.yml             # Dependencies: dbt_expectations >=0.10.0
├── models/
│   ├── sources.yml          # 8 raw schemas (raw_hr, raw_pos, raw_timeclock, ...)
│   ├── staging/             # 27 views + staging.yml (1:1 with source tables)
│   │   ├── stg_*.sql        # One view per source table
│   │   └── staging.yml      # Column tests + dbt_expectations data tests
│   └── marts/               # 29 tables + marts.yml
│       ├── mart_*.sql       # Domain-aggregated tables
│       └── marts.yml        # Column tests + grain documentation
├── tests/                   # 10 custom assert_* tests (data quality)
├── macros/                  # Empty (no custom macros)
└── target/                  # Build artifacts (gitignored)
```

## Where to Look

| Task | Location | Notes |
|------|----------|-------|
| Add a new staging model | `models/staging/stg_<schema>_<table>.sql` | Follow `with source as (select * from {{ source('raw_<schema>', '<table>') }})` pattern |
| Add a new mart model | `models/marts/mart_<name>.sql` | Document grain in marts.yml comment |
| Add source table | `models/sources.yml` | Add to correct `raw_<schema>` source block |
| Add column test | `staging.yml` or `marts.yml` | Use `dbt_expectations` for data tests |
| Add custom data quality test | `tests/assert_<name>.sql` | Returns rows on failure |
| Change materialization | `dbt_project.yml` under `models.grocery` | staging=view, marts=table |

## Conventions

### Naming
- Staging: `stg_<schema>_<table>` — matches source table name exactly
- Marts: `mart_<domain>_<description>` — grain documented in YAML comment
- Tests: `assert_<check_description>` — custom tests only; built-in tests go in YAML

### Staging Model Pattern
Every staging model follows this exact shape:
```sql
with source as (
    select * from {{ source('raw_<schema>', '<table>') }}
),
renamed as (
    select
        -- column renames, type casts, derived columns
    from source
)
select * from renamed
```

### Materialization
- **Staging**: views (no storage cost, always fresh)
- **Marts**: tables (refreshed on each `dbt run --select marts`)

### Testing
- **Schema tests** in YAML: `unique`, `not_null`, `relationships`, `accepted_values`
- **Data tests** in YAML: `dbt_expectations` macros (row counts, value ranges, set membership)
- **Custom tests** in `tests/`: `assert_*` SQL that returns rows on failure
- **Severity is TIERED per test** (t_03a3b9fd): default `error` in
  `dbt_project.yml` — a failing test FAILS the DAG run. Critical tier
  (balance checks, FK relationships, e2e propagation) inherits the default.
  Soft tier (range/value-set expectations, known-failing upstream edges)
  opts out with per-test `config: severity: warn` + a comment naming the
  reason and count of known failures.
- **Cross-layer tests carry `tags=['post_marts']`** (e2e 2026-09-06): any
  `tests/*.sql` that `ref()`s a mart model must be tagged — `dbt_test_staging`
  excludes the tag, `dbt_test_marts` selects `marts tag:post_marts`. Without
  it the staging test task races the mart rebuild and fails on stale tables
  after a fresh reseed.
- **Freshness**: `warn_after: 24h`, `error_after: 48h` — freshness errors
  also block the `grocery_dbt` DAG now (`|| true` removed).

### Freshness
All 8 sources have freshness configured: `warn_after: 24h`, `error_after: 48h`. Tracked via `_sdc_extracted_at::timestamp`.

## Anti-PATTERNS

- **Don't** use `ref()` on raw tables — always go through `source()` in staging
- **Don't** materialize staging as tables — they're views by design
- **Don't** neuter the gate: no global `+severity: warn`, no `|| true` on DAG test tasks, no re-adding either. New tests are `error` by default; only demote to `warn` with a comment explaining the known-failure reason (tiered gating, t_03a3b9fd).
- **Don't** add `dbt_packages/` or `target/` to git — they're gitignored
- **Don't** use `execute_values` in dbt — use Jinja loops or `unnest` for bulk inserts
