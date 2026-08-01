-- Attendance & compliance by department / store / day.
-- Grain: one row per (report_date, location_id, department).
--
-- Metrics derived from int_hr_labor_enriched flags (data-lab#28):
--   late_arrival_rate   = share of scheduled shifts with a late clock-in (>5 min)
--   no_show_rate        = share of scheduled shifts with NO attendance
--   overtime_rate       = share of worked shifts exceeding 1.25x scheduled hours
--   break_compliance_rate = share of worked shifts with a complete break pair (>=0.5h)
-- A "scheduled shift" is any row where schedule_status is not null.

{{
    config(
        materialized='table'
    )
}}

with enriched as (
    select * from {{ ref('int_hr_labor_enriched') }}
),

scheduled as (
    select
        scheduled_date as report_date,
        location_id,
        department,
        count(*)                                                 as scheduled_shifts,
        count(*) filter (where attendance_status = 'NO_SHOW')    as no_show_shifts,
        count(*) filter (where is_late_arrival)                  as late_arrival_shifts,
        count(*) filter (where is_overtime)                      as overtime_shifts,
        count(*) filter (where attendance_status in ('ATTENDED','PARTIAL','SHORT')) as worked_shifts,
        count(*) filter (where is_break_compliant)               as break_compliant_shifts
    from enriched
    where schedule_status is not null
    group by scheduled_date, location_id, department
)

select
    report_date,
    location_id,
    department,
    scheduled_shifts,
    no_show_shifts,
    late_arrival_shifts,
    overtime_shifts,
    worked_shifts,
    break_compliant_shifts,
    round(no_show_shifts::numeric    / nullif(scheduled_shifts, 0) * 100, 2) as no_show_rate_pct,
    round(late_arrival_shifts::numeric / nullif(scheduled_shifts, 0) * 100, 2) as late_arrival_rate_pct,
    round(overtime_shifts::numeric    / nullif(worked_shifts, 0)    * 100, 2) as overtime_rate_pct,
    round(break_compliant_shifts::numeric / nullif(worked_shifts, 0) * 100, 2) as break_compliance_rate_pct
from scheduled
order by report_date desc, department
