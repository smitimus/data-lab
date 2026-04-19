-- Product sales performance by day and location.
-- One row per product per location per day.

with items as (
    select * from {{ ref('stg_pos_transaction_items') }}
),

txns as (
    select
        transaction_id,
        transaction_date,
        location_id
    from {{ ref('stg_pos_transactions') }}
),

products as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        department_id,
        unit_cost
    from {{ ref('stg_pos_products') }}
),

departments as (
    select
        department_id,
        department_name
    from {{ ref('stg_pos_departments') }}
),

locations as (
    select
        location_id,
        location_name
    from {{ ref('stg_locations') }}
),

joined as (
    select
        t.transaction_date,
        t.location_id,
        i.transaction_id,
        i.product_id,
        p.product_name,
        p.department_id,
        d.department_name,
        p.category,
        p.subcategory,
        p.unit_cost,
        i.quantity,
        i.unit_price,
        i.discount,
        i.line_total
    from items i
    join txns t        on t.transaction_id  = i.transaction_id
    left join products p    on p.product_id     = i.product_id
    left join departments d on d.department_id  = p.department_id
),

aggregated as (
    select
        transaction_date,
        location_id,
        product_id,
        product_name,
        department_name,
        category,
        subcategory,
        sum(quantity)                                               as units_sold,
        sum(quantity * unit_price)                                  as gross_revenue,
        sum(discount)                                               as total_discounts,
        sum(line_total)                                             as net_revenue,
        count(distinct transaction_id)                              as transaction_count,
        sum(quantity * unit_cost)                                   as total_cost
    from joined
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    a.transaction_date,
    a.location_id,
    l.location_name,
    a.product_id,
    a.product_name,
    a.department_name,
    a.category,
    a.subcategory,
    a.units_sold,
    a.gross_revenue,
    a.total_discounts,
    a.net_revenue,
    a.transaction_count,
    a.total_cost,
    a.net_revenue - a.total_cost                                    as gross_profit,
    round(
        ((a.net_revenue - a.total_cost) / nullif(a.net_revenue, 0) * 100)::numeric,
        2
    )                                                               as margin_pct
from aggregated a
left join locations l on l.location_id = a.location_id
order by a.transaction_date desc, a.net_revenue desc
