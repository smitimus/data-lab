-- Sales patterns by hour of day and day of week.
-- Grain: one row per (hour_of_day, day_of_week, location_id)
--
-- Purpose: Staffing optimization — shows when each store is busiest.
-- Uses hour_of_day and day_of_week computed in stg_pos_transactions.

with transactions as (
    select
        location_id,
        transaction_id,
        total,
        transaction_dt,
        transaction_date,
        hour_of_day,
        day_of_week
    from {{ ref('stg_pos_transactions') }}
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
),

hourly as (
    select
        t.hour_of_day,
        t.day_of_week,
        t.location_id,
        count(distinct t.transaction_id)   as transaction_count,
        count(distinct t.transaction_date) as days_active,
        sum(t.total)                       as total_revenue,
        avg(t.total)                       as avg_transaction_value,
        sum(t.total) / nullif(count(distinct t.transaction_date), 0)
                                           as avg_daily_revenue_at_hour
    from transactions t
    group by t.hour_of_day, t.day_of_week, t.location_id
)

select
    h.hour_of_day,
    h.day_of_week,
    -- Day name for readability
    case h.day_of_week
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end                                              as day_name,
    h.location_id,
    l.location_name,
    l.city,
    l.state,
    h.transaction_count,
    h.days_active,
    h.total_revenue,
    h.avg_transaction_value,
    h.avg_daily_revenue_at_hour,
    -- Share of this store's total weekly revenue that occurs in this hour-slot
    round(100.0 * h.total_revenue / sum(h.total_revenue) over (
        partition by h.location_id
    ), 2)                                            as pct_of_store_revenue,
    -- Rank busiest hours per store (1 = busiest)
    rank() over (
        partition by h.location_id
        order by h.total_revenue desc
    )                                                as revenue_rank
from hourly h
join locations l on l.location_id = h.location_id
order by h.location_id, h.day_of_week, h.hour_of_day
