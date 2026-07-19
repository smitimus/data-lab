-- Labor cost by department: scheduled / actual cost vs revenue proxy by
-- department / store / day. Grain: one row per (report_date, location_id, department).
--
-- Labor cost from mart_department_labor (scheduled_cost / actual_cost).
-- Revenue proxy from mart_department_performance (net_revenue per date/location/department).
--
-- IMPORTANT grain-mismatch note (data-lab#28): HR schedules use free-text department
-- names (bakery, deli, produce, ...) while POS uses department_id (UUIDs) with
-- Title-Cased names (Bakery, Deli, Produce, ...). The two taxonomies do NOT share a
-- key, so the revenue join is a best-effort normalization on LOWER(department_name).
-- Where names don't align (e.g. HR 'store' / 'management' have no POS counterpart),
-- revenue_proxy is null and labor_cost_pct_of_revenue is null. This is a documented
-- source-taxonomy gap, not a modeling bug. (data-lab#11 tracks full department mapping.)
--
-- Source: mart_department_labor + mart_department_performance (data-lab#28).

{{
    config(
        materialized='table'
    )
}}

with dept_labor as (
    select
        report_date,
        location_id,
        department,
        location_name,
        scheduled_hours,
        actual_hours,
        scheduled_cost,
        actual_cost,
        cost_variance,
        attendance_rate_pct
    from {{ ref('mart_department_labor') }}
),

dept_name_map as (
    -- map HR free-text dept -> POS department_id via normalized name
    select
        lower(d.department_name) as dept_key,
        d.department_id
    from {{ ref('stg_pos_departments') }} d
),

revenue as (
    select
        transaction_date,
        location_id,
        department_id,
        net_revenue
    from {{ ref('mart_department_performance') }}
)

select
    dl.report_date,
    dl.location_id,
    dl.location_name,
    dl.department,
    dl.scheduled_hours,
    dl.actual_hours,
    dl.scheduled_cost,
    dl.actual_cost,
    dl.cost_variance,
    dl.attendance_rate_pct,
    coalesce(r.net_revenue, 0)                              as revenue_proxy,
    case
        when coalesce(r.net_revenue, 0) > 0
        then round((dl.actual_cost / r.net_revenue * 100)::numeric, 2)
        else null
    end                                                      as labor_cost_pct_of_revenue
from dept_labor dl
left join dept_name_map m
    on m.dept_key = lower(dl.department)
left join revenue r
    on r.transaction_date = dl.report_date
   and r.location_id      = dl.location_id
   and r.department_id    = m.department_id
order by dl.report_date desc, dl.location_name, dl.department
