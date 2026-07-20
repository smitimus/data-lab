# Cross-Domain Reporting Suite

Executive reporting for the grocery data platform, tying the six data-domain
dashboards into one coherent view. Built as **data-lab#32**.

## Dashboards

| Dashboard | Slug | Domains covered |
|---|---|---|
| **Store — Executive Performance Overview** | `store_performance_exec` | All six (top-line KPIs) |
| Store — Sales, Promotions & Pricing | `store_pos_promotions` | POS & Sales |
| Store — Inventory, Stock & Shrinkage | `store_inventory_shrinkage` | Inventory & Shrinkage |
| Store — Workforce, Attendance & Labor Cost | `store_hr_labor` | HR & Labor |
| Store — Supply Chain & Fulfillment | `store_supply_chain` | Fulfillment / Supply Chain |
| Store — Transport & Fleet | `store_transport_fleet` | Transport & Logistics |
| Store — Loyalty & Retention | `store_loyalty_crm` | Loyalty & CRM |

The Executive Overview surfaces one headline KPI per domain on a single screen:
total revenue, on-hand inventory value, shrink value lost, labor cost, supply-chain
fill rate, transport on-time %, loyalty active rate, and outstanding points liability.

## Refresh cadence

All domain marts are produced by the `grocery_dbt` Airflow DAG (daily). The
Executive Overview reads the same materialized `mart` tables, so it refreshes on
the same daily cadence. Superset charts query the live `grocery` Postgres DB.

## How to add a KPI / domain

Each domain owns a registry entry in `reporting/registry.yml`:

```yaml
- domain: <stable_key>
  dashboard_slug: store_<domain>
  dashboard_title: "Store — <Human Title>"
  kpi_cards: [<metric>, ...]
  datasets_used: [<mart_table>, ...]
  refresh_cadence: daily (grocery_dbt DAG)
```

The per-domain dashboard scripts in `superset/create_missing_dashboards.py`
consume these entries to register datasets and build charts. To add a new
domain: create its marts, append a `reporting/registry.yml` entry, and add a
builder section to `create_missing_dashboards.py`.

## Data dictionary

See `reporting/DATA_DICTIONARY.md` for the mart → business-question mapping per
domain.

## Scheduled reports (executive daily digest)

**Delivery pivoted to S3 (data-lab#35/#36).** Instead of Superset email, the
daily exec digest exports each of dashboard 13's 8 KPI charts as a CSV (plus a
`manifest.json`) into the MinIO `s3://reports/exec-report/<date>/` bucket. The
export runs from the Airflow DAG **`exec_report_s3_export`**
(`airflow/dags/exec_report_s3_export_dag.py`, `airflow/dags/export_exec_report.py`),
scheduled `0 6 * * *`. MinIO is provisioned in `minio/compose.yaml` (bucket
`reports`, creds `admin`/`adminadmin`).

`reporting/schedule_exec_report.py` is now a **readiness/verification** script
(no longer the Superset `/api/v1/report/` registerer). It confirms the
dashboard, the DAG (via CLI in the worker), and that the S3 bucket holds the
expected files for a given date. Use it to satisfy the "verify a scheduled
report delivers" acceptance criterion of #32:

```bash
python3 reporting/schedule_exec_report.py                # verify (no changes)
python3 reporting/schedule_exec_report.py --run          # also trigger a fresh DAG run
python3 reporting/schedule_exec_report.py --date 2026-07-20
```

The script reads dashboard 13 + bucket `reports` and prints
`PASS: executive S3 report pipeline is verified and delivering.` when all
checks pass.

(Superset's built-in `/api/v1/report/` email/PDF blueprint is intentionally
**not** used — Superset 4.1.2's `/chart/data` endpoint is broken server-side,
and the stock image has no headless browser for screenshots. The Airflow +
SQL-Lab + MinIO path is reliable and avoids those pitfalls.)

## Regenerating docs

```
cd airflow/dbt/grocery && dbt docs generate
```
