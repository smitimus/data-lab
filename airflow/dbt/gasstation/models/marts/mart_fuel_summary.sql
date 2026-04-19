-- Fuel sales summary by grade per location per day.

with fuel as (
    select
        transaction_date,
        location_id,
        grade_id,
        grade_name,
        count(*)                    as transaction_count,
        sum(gallons)                as total_gallons,
        sum(total_amount)           as total_revenue,
        avg(price_per_gallon)       as avg_price_per_gallon,
        min(price_per_gallon)       as min_price_per_gallon,
        max(price_per_gallon)       as max_price_per_gallon,
        avg(gallons)                as avg_gallons_per_fill,
        sum(case when has_loyalty_member then 1 else 0 end) as loyalty_fills
    from {{ ref('stg_fuel_transactions') }}
    group by 1, 2, 3, 4
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
)

select
    f.transaction_date,
    f.location_id,
    l.location_name,
    l.city,
    l.state,
    f.grade_id,
    f.grade_name,
    f.transaction_count,
    f.total_gallons,
    f.total_revenue,
    f.avg_price_per_gallon,
    f.min_price_per_gallon,
    f.max_price_per_gallon,
    f.avg_gallons_per_fill,
    f.loyalty_fills,
    round(f.loyalty_fills::numeric / nullif(f.transaction_count, 0) * 100, 1) as loyalty_fill_pct
from fuel f
left join locations l on l.location_id = f.location_id
order by f.transaction_date desc, f.total_revenue desc
