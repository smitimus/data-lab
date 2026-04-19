-- Daily POS revenue summary by location.
-- One row per location per day.

with pos as (
    select
        transaction_date,
        location_id,
        count(*)                                            as pos_transaction_count,
        sum(total)                                          as pos_revenue,
        avg(total)                                          as pos_avg_transaction,
        sum(case when has_loyalty_member then 1 else 0 end) as loyalty_transactions,
        sum(coupon_savings)                                 as coupon_savings_total,
        sum(deal_savings)                                   as deal_savings_total
    from {{ ref('stg_pos_transactions') }}
    group by 1, 2
),

locations as (
    select
        location_id,
        location_name,
        city,
        state,
        location_type
    from {{ ref('stg_locations') }}
)

select
    p.transaction_date,
    p.location_id,
    l.location_name,
    l.city,
    l.state,
    l.location_type,
    p.pos_transaction_count,
    p.pos_revenue,
    p.pos_avg_transaction,
    p.loyalty_transactions,
    p.coupon_savings_total,
    p.deal_savings_total,
    p.coupon_savings_total + p.deal_savings_total           as total_discount_savings,
    p.pos_revenue                                           as total_revenue
from pos p
left join locations l on l.location_id = p.location_id
order by p.transaction_date desc, p.pos_revenue desc
