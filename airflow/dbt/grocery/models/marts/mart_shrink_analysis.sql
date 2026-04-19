-- Shrinkage by product, location, and type — daily aggregation
with shrinkage as (
    select * from {{ ref('stg_inv_shrinkage_events') }}
),

products as (
    select
        product_id,
        product_name,
        sku,
        category
    from {{ ref('stg_pos_products') }}
),

locations as (
    select
        location_id,
        location_name,
        city,
        state
    from {{ ref('stg_locations') }}
),

daily_shrink as (
    select
        s.event_date,
        s.location_id,
        l.location_name,
        l.city,
        l.state,
        s.product_id,
        p.product_name,
        p.sku,
        p.category,
        s.shrinkage_type,          -- aliased from reason in staging
        count(*)                                            as event_count,
        sum(s.quantity_lost)                                as total_quantity_lost,
        sum(s.estimated_value_lost)::numeric(12,2)          as total_value_lost  -- aliased from estimated_cost
    from shrinkage s
    left join products  p on p.product_id  = s.product_id
    left join locations l on l.location_id = s.location_id
    group by
        s.event_date, s.location_id, l.location_name, l.city, l.state,
        s.product_id, p.product_name, p.sku, p.category,
        s.shrinkage_type
)

select * from daily_shrink
order by event_date desc, total_value_lost desc
