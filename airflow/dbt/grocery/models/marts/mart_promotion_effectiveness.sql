-- Weekly ad promotion effectiveness: ad items vs. non-ad transaction volumes,
-- with promo lift vs baseline.
-- Grain: one row per (ad_week_start, ad_week_end, ad_name, product_id, is_ad_item,
--         discount_pct, promoted_price).
-- Source: stg_pricing_weekly_ads, stg_pricing_ad_items, stg_pos_transaction_items,
--         stg_pos_transactions, stg_pos_products (data-lab#26).
--
-- promo_lift_vs_baseline_pct compares a product's on-ad units to its average
-- off-ad units across all weeks (baseline). Positive = ad weeks sell more.

with ads as (
    select * from {{ ref('stg_pricing_weekly_ads') }}
),

ad_items as (
    select * from {{ ref('stg_pricing_ad_items') }}
),

txn_items as (
    select * from {{ ref('stg_pos_transaction_items') }}
),

transactions as (
    select
        transaction_id,
        transaction_date    as sale_date
    from {{ ref('stg_pos_transactions') }}
),

products as (
    select
        product_id,
        product_name,
        sku,
        category
    from {{ ref('stg_pos_products') }}
),

labeled as (
    select
        t.sale_date,
        a.ad_id,
        a.ad_name,
        a.start_date                                    as ad_week_start,
        a.end_date                                      as ad_week_end,
        ti.product_id,
        p.product_name,
        p.category,
        ai.discount_pct,
        ai.promoted_price,
        ti.quantity,
        ti.unit_price,
        ti.line_total,
        case when ai.ad_item_id is not null then true else false end as is_ad_item
    from txn_items ti
    join transactions t on t.transaction_id = ti.transaction_id
    left join ads a
        on t.sale_date between a.start_date and a.end_date
    left join ad_items ai
        on ai.product_id = ti.product_id
       and ai.ad_id      = a.ad_id
    left join products p on p.product_id = ti.product_id
),

weekly_summary as (
    select
        ad_week_start,
        ad_week_end,
        ad_name,
        product_id,
        product_name,
        category,
        is_ad_item,
        discount_pct,
        promoted_price,
        count(distinct sale_date)               as days_active,
        sum(quantity)                           as total_units_sold,
        sum(line_total)::numeric(14,2)          as total_revenue,
        avg(unit_price)::numeric(10,2)          as avg_unit_price
    from labeled
    where ad_week_start is not null
    group by
        ad_week_start, ad_week_end, ad_name, product_id, product_name,
        category, is_ad_item, discount_pct, promoted_price
),

baseline as (
    select
        product_id,
        avg(nullif(total_units_sold, 0))        as baseline_units
    from weekly_summary
    where is_ad_item = false
    group by product_id
)

select
    w.ad_week_start,
    w.ad_week_end,
    w.ad_name,
    w.product_id,
    w.product_name,
    w.category,
    w.is_ad_item,
    w.discount_pct,
    w.promoted_price,
    w.days_active,
    w.total_units_sold,
    w.total_revenue,
    w.avg_unit_price,
    coalesce(
        round(
            ((w.total_units_sold - b.baseline_units)
             / nullif(b.baseline_units, 0) * 100)::numeric,
            2
        ),
        0
    )                                           as promo_lift_vs_baseline_pct
from weekly_summary w
left join baseline b on b.product_id = w.product_id
order by ad_week_start desc, is_ad_item desc, total_units_sold desc
