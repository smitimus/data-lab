-- All-time performance rollup per location.
-- One row per location — good for leaderboard / comparison dashboards.

with daily as (
    select
        location_id,
        location_name,
        city,
        state,
        location_type,
        min(transaction_date)                   as first_sale_date,
        max(transaction_date)                   as last_sale_date,
        count(distinct transaction_date)        as days_with_sales,
        sum(pos_transaction_count)              as total_pos_transactions,
        sum(pos_revenue)                        as total_pos_revenue,
        sum(loyalty_transactions)               as total_loyalty_transactions,
        sum(total_revenue)                      as total_revenue,
        sum(coupon_savings_total)               as total_coupon_savings,
        sum(deal_savings_total)                 as total_deal_savings
    from {{ ref('mart_daily_revenue') }}
    group by 1, 2, 3, 4, 5
)

select
    location_id,
    location_name,
    city,
    state,
    location_type,
    first_sale_date,
    last_sale_date,
    days_with_sales,
    total_pos_transactions,
    total_pos_revenue,
    total_loyalty_transactions,
    total_revenue,
    round(
        (total_revenue / nullif(days_with_sales, 0))::numeric,
        2
    )                                           as avg_daily_revenue,
    round(
        total_loyalty_transactions::numeric
        / nullif(total_pos_transactions, 0) * 100,
        1
    )                                           as loyalty_attach_rate_pct,
    total_coupon_savings,
    total_deal_savings
from daily
order by total_revenue desc
