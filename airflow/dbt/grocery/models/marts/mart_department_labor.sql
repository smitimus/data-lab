-- Department-level labor: daily staffing, scheduled vs. actual hours,
-- attendance rates, and labor cost per department per location.
-- One row per department per location per day.

with employees as (
    select employee_id, hourly_rate
    from {{ ref('stg_employees') }}
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
),

dept_agg as (
    select
        s.scheduled_date,
        s.location_id,
        s.department,
        count(*)                                                    as total_shifts,
        count(case when s.status = 'completed'  then 1 end)        as completed_shifts,
        count(case when s.status = 'called_out' then 1 end)        as called_out_shifts,
        count(case when s.status = 'no_show'    then 1 end)        as no_show_shifts,
        count(distinct s.employee_id)                               as employees_scheduled,
        -- shift duration computed inline
        sum(
            extract(epoch from (s.shift_end::time - s.shift_start::time)) / 3600.0
        )::numeric(8, 2)                                            as scheduled_hours,
        sum(case when s.status = 'completed'
            then extract(epoch from (s.shift_end::time - s.shift_start::time)) / 3600.0
            else 0 end
        )::numeric(8, 2)                                            as actual_hours,
        sum(
            e.hourly_rate
            * extract(epoch from (s.shift_end::time - s.shift_start::time)) / 3600.0
        )::numeric(10, 2)                                           as scheduled_cost,
        sum(case when s.status = 'completed'
            then e.hourly_rate
                 * extract(epoch from (s.shift_end::time - s.shift_start::time)) / 3600.0
            else 0 end
        )::numeric(10, 2)                                           as actual_cost
    from {{ ref('stg_hr_schedules') }} s
    left join employees e on e.employee_id = s.employee_id
    where s.status <> 'scheduled'
    group by s.scheduled_date, s.location_id, s.department
)

select
    da.scheduled_date                                               as report_date,
    da.location_id,
    l.location_name,
    l.city,
    l.state,
    da.department,
    da.employees_scheduled,
    da.total_shifts,
    da.completed_shifts,
    da.called_out_shifts,
    da.no_show_shifts,
    round(da.completed_shifts::numeric / nullif(da.total_shifts, 0) * 100, 1)
                                                                    as attendance_rate_pct,
    da.scheduled_hours,
    da.actual_hours,
    round(da.actual_hours / nullif(da.scheduled_hours, 0) * 100, 1)
                                                                    as hours_utilization_pct,
    da.scheduled_cost,
    da.actual_cost,
    round(da.actual_cost - da.scheduled_cost, 2)                   as cost_variance
from dept_agg da
left join locations l on l.location_id = da.location_id
order by report_date desc, location_name, department
