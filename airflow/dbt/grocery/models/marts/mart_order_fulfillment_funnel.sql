-- End-to-end order lifecycle: store order → fulfillment → delivery
-- Grain: one row per store_order_id

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
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
),

final as (
    select distinct on (o.order_id)
        o.order_id,
        o.order_date,
        o.order_dt,
        o.store_location_id,
        sl.location_name                            as store_name,
        sl.city                                     as store_city,
        sl.state                                    as store_state,
        o.warehouse_location_id,
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
            when f.fulfillment_status = 'completed'
                and l.load_status = 'delivered'
                then 'COMPLETE'
            when f.fulfillment_status = 'completed'
                then 'FULFILLED_PENDING_DELIVERY'
            when l.load_status = 'in_transit'
                then 'IN_TRANSIT'
            when l.load_id is not null
                then 'ASSIGNED_TO_LOAD'
            when f.fulfillment_id is not null
                then 'IN_FULFILLMENT'
            else 'ORDER_PLACED'
        end                                         as pipeline_stage
    from orders o
    left join store_locs sl on sl.location_id = o.store_location_id
    left join items i on i.order_id = o.order_id
    left join fulfillment f on f.store_order_id = o.order_id
    left join loads l on l.store_order_id = o.order_id
    order by o.order_id, f.fulfillment_id nulls last, l.load_id nulls last
)

select * from final
order by order_date desc, order_id
