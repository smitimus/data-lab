-- Supply chain pipeline summary: store order → fulfillment → transport → receipt.
-- One row per store order. Left joins so in-progress orders still appear.

with orders as (
    select
        order_id,
        order_date,
        store_location_id,
        warehouse_location_id,
        status                      as order_status,
        order_dt
    from {{ ref('stg_ordering_store_orders') }}
),

fulfillment as (
    select
        store_order_id,
        fulfillment_id,
        status                      as fulfillment_status,
        hours_to_fulfill,
        completed_at                as fulfillment_completed_at
    from {{ ref('stg_fulfillment_orders') }}
),

loads as (
    select
        li.store_order_id,
        tl.load_id,
        tl.status                   as load_status,
        tl.departed_at,
        tl.arrived_at,
        tl.hours_in_transit,
        tl.load_date
    from {{ ref('stg_transport_load_items') }} li
    join {{ ref('stg_transport_loads') }} tl on tl.load_id = li.load_id
),

store_locs as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
)

select
    o.order_id,
    o.order_date,
    o.order_dt,
    o.store_location_id,
    sl.location_name                                        as store_name,
    sl.city,
    sl.state,
    o.order_status,
    f.fulfillment_id,
    f.fulfillment_status,
    f.hours_to_fulfill,
    l.load_id,
    l.load_status,
    l.departed_at,
    l.arrived_at,
    l.hours_in_transit
from orders o
left join store_locs sl  on sl.location_id   = o.store_location_id
left join fulfillment f  on f.store_order_id = o.order_id
left join loads l        on l.store_order_id = o.order_id
order by o.order_date desc, o.order_dt desc
