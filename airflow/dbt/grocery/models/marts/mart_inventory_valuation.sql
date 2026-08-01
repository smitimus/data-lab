-- Inventory valuation: on-hand value at cost by store / department.
-- Grain: one row per (location_id, department_id) -- store-level rollup per department.
--
-- value_on_hand = SUM(stock_levels.quantity_on_hand * stg_pos_products.unit_cost)
-- Product -> department mapping from stg_pos_products (data-lab#5 catalog join).

{{
    config(
        materialized='table'
    )
}}

with stock as (
    select
        product_id,
        location_id,
        quantity_on_hand,
        quantity_available
    from {{ ref('stg_inv_stock_levels') }}
),

products as (
    select
        product_id,
        department_id,
        department_name,
        unit_cost
    from {{ ref('stg_pos_products') }}
),

locations as (
    select
        location_id,
        location_name,
        location_type
    from {{ ref('stg_locations') }}
)

select
    s.location_id,
    l.location_name,
    l.location_type,
    p.department_id,
    p.department_name,
    sum(s.quantity_on_hand)                                  as units_on_hand,
    sum(s.quantity_available)                                as units_available,
    sum(s.quantity_on_hand * coalesce(p.unit_cost, 0))::numeric(14,2)
                                                                 as value_on_hand_cost,
    sum(s.quantity_available * coalesce(p.unit_cost, 0))::numeric(14,2)
                                                                 as value_available_cost
from stock s
left join products p  on p.product_id  = s.product_id
left join locations l on l.location_id = s.location_id
group by
    s.location_id, l.location_name, l.location_type,
    p.department_id, p.department_name
order by value_on_hand_cost desc
