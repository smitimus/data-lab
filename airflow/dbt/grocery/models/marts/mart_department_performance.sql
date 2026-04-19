-- Revenue and units sold by department per day per location.
-- One row per department per location per day.

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
        i.department_id,
        i.transaction_id,
        i.quantity,
        i.unit_price,
        i.discount,
        i.line_total
    from items i
    join txns t on t.transaction_id = i.transaction_id
),

aggregated as (
    select
        transaction_date,
        location_id,
        department_id,
        count(distinct transaction_id)              as transaction_count,
        sum(quantity)                               as units_sold,
        sum(quantity * unit_price)                  as gross_revenue,
        sum(discount)                               as total_discounts,
        sum(line_total)                             as net_revenue
    from joined
    group by 1, 2, 3
)

select
    a.transaction_date,
    a.location_id,
    l.location_name,
    a.department_id,
    d.department_name,
    a.transaction_count,
    a.units_sold,
    a.gross_revenue,
    a.total_discounts,
    a.net_revenue,
    round(
        (a.gross_revenue / nullif(a.transaction_count, 0))::numeric,
        2
    )                                               as avg_basket_size
from aggregated a
left join locations l   on l.location_id   = a.location_id
left join departments d on d.department_id = a.department_id
order by a.transaction_date desc, a.net_revenue desc
