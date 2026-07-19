-- Category sales performance: revenue, units, and basket size by category / store / day.
-- Grain: one row per (transaction_date, location_id, category).
-- Source: int_pos_sales_enriched (data-lab#26).

{{
    config(
        materialized='table'
    )
}}

with enriched as (
    select * from {{ ref('int_pos_sales_enriched') }}
),

aggregated as (
    select
        transaction_date,
        location_id,
        category,
        sum(line_total)                        as category_revenue,
        sum(quantity)                          as category_units,
        count(distinct transaction_id)         as transaction_count
    from enriched
    group by transaction_date, location_id, category
)

select
    transaction_date,
    location_id,
    category,
    category_revenue,
    category_units,
    transaction_count,
    round(
        (category_revenue / nullif(transaction_count, 0))::numeric,
        2
    )                                           as avg_basket_size
from aggregated
order by transaction_date desc, category_revenue desc
