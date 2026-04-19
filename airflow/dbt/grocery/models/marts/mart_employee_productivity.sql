-- Employee productivity: POS transactions processed and revenue driven per employee per day.
-- Joins timeclock clock_in/clock_out pairs to estimate hours on the floor.
-- One row per employee per day.

with transactions as (
    select
        transaction_id,
        employee_id,
        transaction_date    as sale_date,
        location_id,
        total
    from {{ ref('stg_pos_transactions') }}
    where employee_id is not null
),

employees as (
    select
        employee_id,
        location_id,
        full_name,
        department,
        job_title,
        hourly_rate,
        status
    from {{ ref('stg_employees') }}
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
),

-- Approximate hours worked: first clock_in to last clock_out per employee per day
timeclock as (
    select
        employee_id,
        event_date,
        min(event_dt) filter (where event_type = 'clock_in')    as first_clock_in,
        max(event_dt) filter (where event_type = 'clock_out')   as last_clock_out,
        sum(extract(epoch from event_dt) / 3600.0)
            filter (where event_type = 'break_start')           as break_start_sum,
        sum(extract(epoch from event_dt) / 3600.0)
            filter (where event_type = 'break_end')             as break_end_sum
    from {{ ref('stg_timeclock_events') }}
    group by employee_id, event_date
),

hours_worked as (
    select
        employee_id,
        event_date,
        round(
            extract(epoch from (last_clock_out - first_clock_in)) / 3600.0
            -- subtract break time (break_end - break_start offset approximated as 0.5h if breaks exist)
            - case when break_start_sum is not null then 0.5 else 0 end,
            2
        )::numeric(8, 2)                                        as hours_worked
    from timeclock
    where first_clock_in is not null and last_clock_out is not null
        and last_clock_out > first_clock_in
),

-- Aggregate POS transactions per employee per day
txn_daily as (
    select
        employee_id,
        location_id,
        sale_date,
        count(*)                                    as transaction_count,
        sum(total)::numeric(14, 2)                  as total_revenue_processed
    from transactions
    group by employee_id, location_id, sale_date
),

joined as (
    select
        t.sale_date,
        t.employee_id,
        e.full_name,
        e.department,
        e.job_title,
        e.hourly_rate,
        e.status,
        t.location_id,
        l.location_name,
        l.city,
        l.state,
        t.transaction_count,
        t.total_revenue_processed,
        h.hours_worked,
        case
            when h.hours_worked > 0
            then round(t.total_revenue_processed / h.hours_worked, 2)
        end                                             as revenue_per_hour,
        case
            when h.hours_worked > 0
            then round(t.transaction_count / h.hours_worked, 2)
        end                                             as transactions_per_hour,
        round((e.hourly_rate * coalesce(h.hours_worked, 0))::numeric, 2)
                                                        as estimated_labor_cost,
        case
            when t.total_revenue_processed > 0
            then round(
                    ((e.hourly_rate * coalesce(h.hours_worked, 0))
                    / t.total_revenue_processed * 100)::numeric,
                    2
                 )
        end                                             as labor_cost_pct_revenue
    from txn_daily t
    left join employees  e on e.employee_id = t.employee_id
    left join locations  l on l.location_id = t.location_id
    left join hours_worked h
           on h.employee_id = t.employee_id
          and h.event_date  = t.sale_date
)

select * from joined
order by sale_date desc, total_revenue_processed desc
