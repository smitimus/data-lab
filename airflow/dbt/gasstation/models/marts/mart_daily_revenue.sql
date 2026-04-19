-- Daily revenue summary across POS and fuel, joined with location info.
-- One row per location per day.

with pos as (
    select
        transaction_date,
        location_id,
        count(*)            as pos_transaction_count,
        sum(subtotal)       as pos_subtotal,
        sum(tax)            as pos_tax,
        sum(total)          as pos_revenue,
        avg(total)          as pos_avg_transaction,
        sum(case when has_loyalty_member then 1 else 0 end) as loyalty_transactions
    from {{ ref('stg_pos_transactions') }}
    group by 1, 2
),

fuel as (
    select
        transaction_date,
        location_id,
        count(*)            as fuel_transaction_count,
        sum(gallons)        as fuel_gallons_sold,
        sum(total_amount)   as fuel_revenue,
        avg(price_per_gallon) as fuel_avg_price_per_gallon
    from {{ ref('stg_fuel_transactions') }}
    group by 1, 2
),

locations as (
    select location_id, location_name, city, state, location_type
    from {{ ref('stg_locations') }}
),

combined as (
    select
        coalesce(p.transaction_date, f.transaction_date)    as transaction_date,
        coalesce(p.location_id, f.location_id)              as location_id,
        coalesce(p.pos_transaction_count, 0)                as pos_transaction_count,
        coalesce(p.pos_revenue, 0)                          as pos_revenue,
        coalesce(p.pos_avg_transaction, 0)                  as pos_avg_transaction,
        coalesce(p.loyalty_transactions, 0)                 as loyalty_transactions,
        coalesce(f.fuel_transaction_count, 0)               as fuel_transaction_count,
        coalesce(f.fuel_gallons_sold, 0)                    as fuel_gallons_sold,
        coalesce(f.fuel_revenue, 0)                         as fuel_revenue,
        coalesce(f.fuel_avg_price_per_gallon, 0)            as fuel_avg_price_per_gallon
    from pos p
    full outer join fuel f
        on f.transaction_date = p.transaction_date
       and f.location_id = p.location_id
)

select
    c.transaction_date,
    c.location_id,
    l.location_name,
    l.city,
    l.state,
    l.location_type,
    c.pos_transaction_count,
    c.pos_revenue,
    c.pos_avg_transaction,
    c.loyalty_transactions,
    c.fuel_transaction_count,
    c.fuel_gallons_sold,
    c.fuel_revenue,
    c.fuel_avg_price_per_gallon,
    c.pos_revenue + c.fuel_revenue                          as total_revenue,
    c.pos_transaction_count + c.fuel_transaction_count      as total_transactions
from combined c
left join locations l on l.location_id = c.location_id
order by c.transaction_date desc, total_revenue desc
