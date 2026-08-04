-- HR/labor intermediate: schedules + timeclock pairs + actual-vs-scheduled hours
-- + employee cost, one row per (employee_id, scheduled_date, location_id).
-- Grain: one row per (employee_id, scheduled_date, location_id).
--
-- Joins stg_hr_schedules (shift_start/end, scheduled/actual hours) to attendance
-- (clock in/out, net hours) via mart_attendance_summary, plus stg_employees
-- (hourly_rate) and stg_locations. employee_cost = net_hours_worked * hourly_rate.
--
-- Late-arrival / overtime / break-compliance flags are derived here and consumed by
-- mart_attendance_compliance. Late threshold = clock_in > shift_start + 5 min.
-- Overtime = net_hours > scheduled_hours * 1.25 (only when both present).
-- Break compliance = has_complete_pair (from attendance pivot) AND net_break >= 0.5h.

{{
    config(
        materialized='table'
    )
}}

with schedules as (
    select
        employee_id,
        scheduled_date,
        location_id,
        department,
        shift_start,
        shift_end,
        scheduled_hours,
        actual_hours,
        status as schedule_status
    from {{ ref('stg_hr_schedules') }}
),

attendance as (
    -- Pivot timeclock events into in/out pairs per employee per day.
    -- Computes clock_in_at, clock_out_at, total_clocked_hours, total_break_hours,
    -- net_hours_worked, has_complete_pair, has_unpaired_events from raw events.
    with paired as (
        select
            employee_id,
            event_date,
            location_id,
            min(case when event_type = 'clock_in' then event_dt end)  as clock_in_at,
            max(case when event_type = 'clock_out' then event_dt end) as clock_out_at,
            count(distinct case when event_type = 'clock_in' then event_id end) as ins,
            count(distinct case when event_type = 'clock_out' then event_id end) as outs
        from {{ ref('stg_timeclock_events') }}
        group by employee_id, event_date, location_id
    ),
    breaks as (
        -- Sum each complete break_start/break_end pair per employee per day.
        -- Pair each break_start with the next chronological break_end for the
        -- same employee/day/location.
        select
            bs.employee_id,
            bs.event_date,
            bs.location_id,
            sum(
                extract(epoch from (be.break_end_dt - bs.break_start)) / 3600.0
            ) as total_break_hours
        from (
            select
                employee_id,
                event_date,
                location_id,
                event_dt as break_start,
                row_number() over (
                    partition by employee_id, event_date, location_id
                    order by event_dt
                ) as rn
            from {{ ref('stg_timeclock_events') }}
            where event_type = 'break_start'
        ) bs
        join (
            select
                employee_id,
                event_date,
                location_id,
                event_dt as break_end_dt,
                row_number() over (
                    partition by employee_id, event_date, location_id
                    order by event_dt
                ) as rn
            from {{ ref('stg_timeclock_events') }}
            where event_type = 'break_end'
        ) be
          on be.employee_id = bs.employee_id
         and be.event_date   = bs.event_date
         and be.location_id  = bs.location_id
         and be.rn          = bs.rn
        group by bs.employee_id, bs.event_date, bs.location_id
    )
    select
        p.employee_id,
        p.event_date,
        p.location_id,
        p.clock_in_at,
        p.clock_out_at,
        case
            when p.clock_in_at is not null and p.clock_out_at is not null
            then extract(epoch from (p.clock_out_at - p.clock_in_at)) / 3600.0
        end as total_clocked_hours,
        coalesce(b.total_break_hours, 0) as total_break_hours,
        case
            when p.clock_in_at is not null and p.clock_out_at is not null
            then extract(epoch from (p.clock_out_at - p.clock_in_at)) / 3600.0
        end as net_hours_worked,
        (p.ins > 0 and p.outs > 0) as has_complete_pair,
        (p.ins != p.outs) as has_unpaired_events
    from paired p
    left join breaks b
      on b.employee_id = p.employee_id
     and b.event_date   = p.event_date
     and b.location_id  = p.location_id
),

employees as (
    select
        employee_id,
        full_name,
        department as emp_department,
        job_title,
        hourly_rate,
        is_active
    from {{ ref('stg_employees') }}
),

locations as (
    select
        location_id,
        location_name,
        city,
        state,
        location_type
    from {{ ref('stg_locations') }}
),

combined as (
    select
        coalesce(s.employee_id, a.employee_id)    as employee_id,
        coalesce(s.scheduled_date, a.event_date)   as scheduled_date,
        coalesce(s.location_id, a.location_id)     as location_id,
        coalesce(s.department, e.emp_department)   as department,
        e.full_name,
        e.job_title,
        e.hourly_rate,
        s.shift_start,
        s.shift_end,
        s.scheduled_hours,
        s.actual_hours                              as schedule_actual_hours,
        s.schedule_status,
        a.clock_in_at,
        a.clock_out_at,
        a.total_clocked_hours,
        a.total_break_hours,
        a.net_hours_worked,
        a.has_complete_pair,
        a.has_unpaired_events,
        l.location_name,
        l.city,
        l.state,
        l.location_type,
        case
            when a.clock_in_at is not null
             and s.shift_start is not null
             and (a.clock_in_at::time) > (s.shift_start + interval '5 minutes')
            then true else false
        end as is_late_arrival,
        case
            when a.net_hours_worked is not null and s.scheduled_hours is not null
             and a.net_hours_worked > s.scheduled_hours * 1.25
            then true else false
        end as is_overtime,
        case
            when a.has_complete_pair and coalesce(a.total_break_hours, 0) >= 0.5
            then true else false
        end as is_break_compliant,
        case
            when s.employee_id is not null and a.employee_id is null
                then 'NO_SHOW'
            when a.employee_id is not null and s.employee_id is null
                then 'UNSCHEDULED'
            when a.net_hours_worked is not null and s.scheduled_hours is not null
                then
                    case
                        when a.net_hours_worked >= s.scheduled_hours * 0.9 then 'ATTENDED'
                        when a.net_hours_worked < s.scheduled_hours * 0.5 then 'PARTIAL'
                        else 'SHORT'
                    end
            else 'UNKNOWN'
        end as attendance_status,
        case
            when a.net_hours_worked is not null and e.hourly_rate is not null
            then round((a.net_hours_worked * e.hourly_rate)::numeric, 2)
        end as employee_cost,
        case
            when a.net_hours_worked is not null and s.scheduled_hours is not null
            then round((a.net_hours_worked / nullif(s.scheduled_hours, 0) * 100)::numeric, 1)
        end as hours_attainment_pct
    from schedules s
    full outer join attendance a
        on a.employee_id = s.employee_id
       and a.event_date   = s.scheduled_date
       and a.location_id  = s.location_id
    left join employees e on e.employee_id = coalesce(s.employee_id, a.employee_id)
    left join locations l on l.location_id = coalesce(s.location_id, a.location_id)
)

select * from combined
order by scheduled_date desc, employee_id
