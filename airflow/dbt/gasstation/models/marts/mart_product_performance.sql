-- Product performance: units sold, revenue, and margin per product per day.

with items as (
    select
        i.product_id,
        i.category,
        i.subcategory,
        t.transaction_date,
        t.location_id,
        sum(i.quantity)     as units_sold,
        sum(i.line_total)   as gross_revenue,
        sum(i.discount * i.quantity) as total_discounts,
        count(distinct i.transaction_id) as transaction_count
    from {{ ref('stg_pos_transaction_items') }} i
    join {{ ref('stg_pos_transactions') }} t
        on t.transaction_id = i.transaction_id
    group by 1, 2, 3, 4, 5
),

products as (
    select product_id, product_name, unit_cost, unit_price, margin_pct
    from {{ ref('stg_pos_products') }}
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
)

select
    i.transaction_date,
    i.location_id,
    l.location_name,
    i.product_id,
    p.product_name,
    i.category,
    i.subcategory,
    i.units_sold,
    i.gross_revenue,
    i.total_discounts,
    i.gross_revenue - i.total_discounts             as net_revenue,
    i.transaction_count,
    p.unit_cost * i.units_sold                      as total_cost,
    i.gross_revenue - (p.unit_cost * i.units_sold)  as gross_profit,
    p.margin_pct
from items i
left join products p  on p.product_id  = i.product_id
left join locations l on l.location_id = i.location_id
order by i.transaction_date desc, i.gross_revenue desc
