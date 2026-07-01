-- Employee hours worked vs scheduled — cross-org between HR and Timeclock
-- Grain: one row per (employee_id, event_date)

with attendance as (
    select
        employee_id,
        event_date,
        location_id,
        net_hours_worked,
        total_clocked_hours,
        total_break_hours,
        clock_in_at,
        clock_out_at,
        has_complete_pair,
        has_unpaired_events
    from {{ ref('mart_attendance_summary') }}
),

schedules as (
    select
        employee_id,
        scheduled_date,
        location_id,
        scheduled_hours,
        actual_hours,
        status as schedule_status
    from {{ ref('stg_hr_schedules') }}
),

employees as (
    select employee_id, full_name, department, job_title, hourly_rate, is_active
    from {{ ref('stg_employees') }}
),

combined as (
    select
        coalesce(a.event_date, s.scheduled_date) as report_date,
        coalesce(a.employee_id, s.employee_id)   as employee_id,
        coalesce(a.location_id, s.location_id)   as location_id,
        e.full_name,
        e.department,
        e.job_title,
        e.hourly_rate,
        e.is_active,
        s.scheduled_hours,
        s.actual_hours                          as schedule_actual_hours,
        s.schedule_status,
        a.total_clocked_hours,
        a.net_hours_worked,
        a.total_break_hours,
        a.clock_in_at,
        a.clock_out_at,
        a.has_complete_pair,
        a.has_unpaired_events,
        case
            when s.employee_id is not null and a.employee_id is null
                then 'NO_SHOW'
            when a.employee_id is not null and s.employee_id is null
                then 'UNSCHEDULED'
            when a.net_hours_worked is not null
                 and s.scheduled_hours is not null
                 and a.net_hours_worked >= s.scheduled_hours * 0.9
                then 'ATTENDED'
            when a.net_hours_worked is not null
                 and s.scheduled_hours is not null
                 and a.net_hours_worked < s.scheduled_hours * 0.5
                then 'PARTIAL'
            when a.net_hours_worked is not null
                 and s.scheduled_hours is not null
                then 'SHORT'
            else 'UNKNOWN'
        end as attendance_status,
        case
            when a.net_hours_worked is not null and s.scheduled_hours is not null
                then round((a.net_hours_worked / s.scheduled_hours * 100)::numeric, 1)
        end as hours_attainment_pct,
        case
            when a.net_hours_worked is not null and e.hourly_rate is not null
                then round((a.net_hours_worked * e.hourly_rate)::numeric, 2)
        end as estimated_labor_cost
    from attendance a
    full outer join schedules s
        on s.employee_id = a.employee_id
        and s.scheduled_date = a.event_date
        and s.location_id = a.location_id
    left join employees e
        on e.employee_id = coalesce(a.employee_id, s.employee_id)
)

select * from combined
order by report_date desc, employee_id
