-- All-time location performance rollup — one row per location.
-- Good for leaderboard / comparison dashboards in Superset.

with daily as (
    select
        location_id,
        location_name,
        city,
        state,
        location_type,
        min(transaction_date)               as first_sale_date,
        max(transaction_date)               as last_sale_date,
        count(distinct transaction_date)    as days_with_sales,
        sum(pos_transaction_count)          as total_pos_transactions,
        sum(pos_revenue)                    as total_pos_revenue,
        sum(loyalty_transactions)           as total_loyalty_transactions,
        sum(fuel_transaction_count)         as total_fuel_transactions,
        sum(fuel_gallons_sold)              as total_gallons_sold,
        sum(fuel_revenue)                   as total_fuel_revenue,
        sum(total_revenue)                  as total_revenue
    from {{ ref('mart_daily_revenue') }}
    group by 1, 2, 3, 4, 5
)

select
    *,
    total_pos_revenue + total_fuel_revenue          as revenue_check,
    round((total_revenue / nullif(days_with_sales, 0))::numeric, 2)    as avg_daily_revenue,
    round(total_loyalty_transactions::numeric
          / nullif(total_pos_transactions, 0) * 100, 1)     as loyalty_attach_rate_pct,
    total_pos_transactions + total_fuel_transactions        as total_transactions
from daily
order by total_revenue desc
