-- Fulfillment / supply-chain enrichment: store orders + items + fulfillment +
-- delivery + pick accuracy, one row per store_order_id.
-- Grain: one row per store_order_id.
--
-- Joins stg_ordering_store_orders → stg_ordering_store_order_items (aggregated by
-- order_id) → stg_fulfillment_orders (via store_order_id) + pick metrics from
-- stg_fulfillment_items → delivery (stg_transport_load_items → stg_transport_loads
-- via load_id) → store/warehouse names (stg_locations).
--
-- Derived durations reused from mart_order_fulfillment_funnel:
--   days_order_to_fulfill, days_order_to_delivery, hours_to_approve.
-- On-time rule (data-lab#29): delivered on/before requested_delivery_dt.
--   on_time = arrived_at::date <= requested_delivery_dt (null when undelivered).
--
-- Source: stg_ordering_store_orders, stg_ordering_store_order_items,
--         stg_fulfillment_orders, stg_fulfillment_items,
--         stg_transport_load_items, stg_transport_loads, stg_locations (data-lab#29).

{{
    config(
        materialized='table'
    )
}}

with orders as (
    select
        order_id,
        store_location_id,
        warehouse_location_id,
        order_dt,
        order_date,
        requested_delivery_dt,
        status                          as order_status,
        approved_dt,
        approved_by
    from {{ ref('stg_ordering_store_orders') }}
),

items as (
    select
        order_id,
        count(*)                                            as order_line_items,
        sum(quantity_requested::integer)                    as total_qty_ordered,
        sum(coalesce(quantity_approved, quantity_requested)::integer) as total_qty_approved
    from {{ ref('stg_ordering_store_order_items') }}
    group by order_id
),

fulfillment as (
    select
        store_order_id,
        fulfillment_id,
        status                      as fulfillment_status,
        hours_to_fulfill,
        started_at,
        completed_at                as fulfillment_completed_at,
        assigned_to_name            as picker_name
    from {{ ref('stg_fulfillment_orders') }}
),

pick as (
    select
        fulfillment_id,
        sum(quantity_requested::integer) as total_qty_requested,
        sum(quantity_picked::integer)    as total_qty_picked,
        sum(case when pick_status = 'shorted' then quantity_requested::integer
                 else 0 end)              as total_shorted_qty
    from {{ ref('stg_fulfillment_items') }}
    group by fulfillment_id
),

loads as (
    select
        li.store_order_id,
        tl.load_id,
        tl.status                   as load_status,
        tl.departed_at,
        tl.arrived_at,
        tl.hours_in_transit,
        tl.load_date,
        tl.from_warehouse,
        tl.to_store,
        tl.driver
    from {{ ref('stg_transport_load_items') }} li
    join {{ ref('stg_transport_loads') }} tl on tl.load_id = li.load_id
),

store_locs as (
    select location_id, location_name, city, state, location_type
    from {{ ref('stg_locations') }}
),

final as (
    select distinct on (o.order_id)
        o.order_id,
        o.order_date,
        o.store_location_id,
        sl.location_name                            as store_name,
        sl.city                                     as store_city,
        sl.state                                    as store_state,
        o.warehouse_location_id,
        wl.location_name                            as warehouse_name,
        o.requested_delivery_dt,
        o.approved_dt,
        o.approved_by,
        o.order_status,
        coalesce(i.order_line_items, 0)             as order_line_items,
        coalesce(i.total_qty_ordered, 0)            as total_qty_ordered,
        coalesce(i.total_qty_approved, 0)           as total_qty_approved,
        f.fulfillment_id,
        f.fulfillment_status,
        f.hours_to_fulfill,
        f.fulfillment_completed_at,
        f.picker_name,
        p.total_qty_requested                       as pick_qty_requested,
        p.total_qty_picked                          as pick_qty_picked,
        p.total_shorted_qty,
        l.load_id,
        l.load_status,
        l.departed_at,
        l.arrived_at,
        l.hours_in_transit,
        l.driver                                    as driver_name,
        l.load_date,
        case
            when f.fulfillment_completed_at is not null and o.order_dt is not null
                then round(
                    extract(epoch from (f.fulfillment_completed_at - o.order_dt)) / 86400.0,
                    1
                )
        end                                         as days_order_to_fulfill,
        case
            when l.arrived_at is not null and o.order_dt is not null
                then round(
                    extract(epoch from (l.arrived_at - o.order_dt)) / 86400.0,
                    1
                )
        end                                         as days_order_to_delivery,
        case
            when o.approved_dt is not null and o.order_dt is not null
                then round(
                    extract(epoch from (o.approved_dt - o.order_dt)) / 3600.0,
                    1
                )
        end                                         as hours_to_approve,
        case
            when l.arrived_at is not null
             and o.requested_delivery_dt is not null
             and l.arrived_at::date <= o.requested_delivery_dt
                then true
            else false
        end                                         as on_time,
        case
            when l.arrived_at is not null
             and o.requested_delivery_dt is not null
                then greatest(0,
                    extract(epoch from (l.arrived_at::timestamp - o.requested_delivery_dt::timestamp)) / 86400.0)
            else null
        end                                         as days_late
    from orders o
    left join store_locs sl on sl.location_id = o.store_location_id
    left join store_locs wl on wl.location_id = o.warehouse_location_id
    left join items i on i.order_id = o.order_id
    left join fulfillment f on f.store_order_id = o.order_id
    left join pick p on p.fulfillment_id = f.fulfillment_id
    left join loads l on l.store_order_id = o.order_id
    order by o.order_id, f.fulfillment_id nulls last, l.load_id nulls last
)

select * from final
order by order_date desc, order_id
