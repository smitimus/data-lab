-- Labor scheduling vs. actuals: attendance rates, no-show/call-out by dept/location
with schedules as (
    select * from {{ ref('stg_hr_schedules') }}
),

employees as (
    select
        employee_id,
        full_name,
        department,
        location_id
    from {{ ref('stg_employees') }}
),

locations as (
    select
        location_id,
        location_name,
        city,
        state
    from {{ ref('stg_locations') }}
),

daily_dept as (
    select
        s.scheduled_date,
        s.location_id,
        l.location_name,
        l.city,
        l.state,
        s.department,
        count(*)                                                    as shifts_scheduled,
        count(*) filter (where s.status = 'completed')              as shifts_completed,
        count(*) filter (where s.status = 'called_out')             as shifts_called_out,
        count(*) filter (where s.status = 'no_show')                as shifts_no_show,
        count(*) filter (where s.status = 'scheduled')              as shifts_pending,
        round(
            count(*) filter (where s.status = 'completed')::numeric
            / nullif(count(*) filter (where s.status <> 'scheduled'), 0) * 100,
            1
        )                                                           as completion_rate_pct,
        round(
            count(*) filter (where s.status = 'no_show')::numeric
            / nullif(count(*) filter (where s.status <> 'scheduled'), 0) * 100,
            1
        )                                                           as no_show_rate_pct
    from schedules s
    left join locations l on l.location_id = s.location_id
    where s.status <> 'scheduled'   -- only resolved shifts
    group by
        s.scheduled_date, s.location_id, l.location_name, l.city, l.state,
        s.department
)

select * from daily_dept
order by scheduled_date desc, location_name, department
