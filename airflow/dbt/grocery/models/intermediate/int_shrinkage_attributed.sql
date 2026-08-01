-- Shrinkage events attributed to product -> department / category / cause.
-- Grain: one row per shrinkage event (event_id).
--
-- Product -> department/category mapping is sourced from stg_pos_products (the product
-- catalog), NOT from transaction_items.department_id -- per data-lab#5 (correct
-- attribution fix). stg_inv_shrinkage_events carries product_id, so the join is clean.

{{
    config(
        materialized='table'
    )
}}

with shrinkage as (
    select * from {{ ref('stg_inv_shrinkage_events') }}
),

products as (
    select
        product_id,
        product_name,
        sku,
        category,
        department_id,
        department_name
    from {{ ref('stg_pos_products') }}
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
    s.event_id,
    s.product_id,
    p.product_name,
    p.sku,
    p.category,
    p.department_id,
    p.department_name,
    s.location_id,
    l.location_name,
    l.city,
    l.state,
    l.location_type,
    s.event_date,
    s.recorded_at,
    s.shrinkage_type,
    s.quantity_lost,
    s.estimated_value_lost
from shrinkage s
left join products p  on p.product_id  = s.product_id
left join locations l on l.location_id = s.location_id
