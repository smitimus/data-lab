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

`reporting/schedule_exec_report.py` wires the Executive Overview dashboard
(slug `store_performance_exec`) to a daily email snapshot (PDF/PNG). It is
idempotent and safe to re-run.

```bash
python3 reporting/schedule_exec_report.py \
  --superset-url http://localhost:8088 \
  --recipient chris@example.com \
  --crontab "0 7 * * *"
```

**Prerequisites (InfraOps — Superset report worker):** the `ALERT_REPORTS`
feature flag must be enabled, a Superset celery worker + celery beat must be
present in the stack (the Airflow worker does not serve Superset reports), and
SMTP must be configured. Until those exist, `/api/v1/report/` is not registered
and the script reports a clear blocker. See the linked data-lab ticket for
enabling scheduled reports.

## Regenerating docs

```
cd airflow/dbt/grocery && dbt docs generate
```
