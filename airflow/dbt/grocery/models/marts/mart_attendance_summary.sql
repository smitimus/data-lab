-- Daily attendance per employee: clock in/out, total hours, break time, net hours
-- Grain: one row per (employee_id, event_date)

with events as (
    select * from {{ ref('stg_timeclock_events') }}
),

-- Pivot: first clock-in, last clock-out, and break pairs per employee per day
attendance as (
    select
        employee_id,
        location_id,
        event_date,
        min(event_dt) filter (where event_type = 'clock_in')
            as clock_in_at,
        max(event_dt) filter (where event_type = 'clock_out')
            as clock_out_at,
        -- Total clocked duration = clock_out - clock_in (handles midnight crossover)
        coalesce(
            extract(epoch from (
                max(event_dt) filter (where event_type = 'clock_out')
                - min(event_dt) filter (where event_type = 'clock_in')
            )) / 3600.0
            + case when max(event_dt) filter (where event_type = 'clock_out') is not null
                     and min(event_dt) filter (where event_type = 'clock_in') is not null
                     and max(event_dt) filter (where event_type = 'clock_out')::time
                         <= min(event_dt) filter (where event_type = 'clock_in')::time
                   then 24.0 else 0 end,
            0
        )::numeric(5, 2) as total_clocked_hours,
        -- Number of events for balance checking
        count(*) filter (where event_type = 'clock_in')   as clock_in_count,
        count(*) filter (where event_type = 'clock_out')  as clock_out_count,
        count(*) filter (where event_type = 'break_start') as break_start_count,
        count(*) filter (where event_type = 'break_end')  as break_end_count
    from events
    group by employee_id, location_id, event_date
),

-- Break durations: pair each break_start with its subsequent break_end
break_pairs as (
    select
        employee_id,
        event_date,
        break_start_at,
        break_end_at,
        extract(epoch from (break_end_at - break_start_at)) / 3600.0 as break_hours
    from (
        select
            employee_id,
            event_date,
            event_dt as break_start_at,
            lead(event_dt) over (
                partition by employee_id, event_date
                order by event_dt
            ) as break_end_at,
            event_type,
            lead(event_type) over (
                partition by employee_id, event_date
                order by event_dt
            ) as next_type
        from events
        where event_type in ('break_start', 'break_end')
    ) paired
    where event_type = 'break_start' and next_type = 'break_end'
),

break_totals as (
    select
        employee_id,
        event_date,
        count(*) as completed_break_pairs,
        sum(break_hours)::numeric(5, 2) as total_break_hours,
        count(*) filter (where break_hours < 0) as negative_break_pairs
    from break_pairs
    group by employee_id, event_date
),

final as (
    select
        a.employee_id,
        a.event_date,
        a.location_id,
        a.clock_in_at,
        a.clock_out_at,
        a.total_clocked_hours,
        coalesce(b.total_break_hours, 0)::numeric(5, 2) as total_break_hours,
        case
            when a.total_clocked_hours > 0
                then (a.total_clocked_hours - coalesce(b.total_break_hours, 0))::numeric(5, 2)
        end as net_hours_worked,
        a.clock_in_count,
        a.clock_out_count,
        a.break_start_count,
        a.break_end_count,
        coalesce(b.completed_break_pairs, 0) as completed_break_pairs,
        case
            when (a.clock_in_count = 1 and a.clock_out_count = 1)
                 or (a.clock_in_count > 0 and a.clock_out_count > 0)
            then true else false
        end as has_complete_pair,
        case
            when a.clock_in_count != a.clock_out_count
                or a.break_start_count != a.break_end_count
            then true else false
        end as has_unpaired_events
    from attendance a
    left join break_totals b
        on b.employee_id = a.employee_id
        and b.event_date = a.event_date
)

select * from final
order by event_date desc, employee_id
