-- Fulfillment order operations: picker performance, cycle time, status tracking
-- Grain: one row per fulfillment_id

with fulfillment as (
    select * from {{ ref('stg_fulfillment_orders') }}
),

items as (
    select
        fulfillment_id,
        count(*)                                    as total_items,
        sum(quantity_requested)                     as total_qty_requested,
        sum(quantity_picked)                        as total_qty_picked,
        count(*) filter (where pick_status = 'short') as shorted_items,
        count(*) filter (where pick_status = 'picked') as fully_picked_items,
        round(
            avg(fill_rate_pct), 1
        )                                           as avg_fill_rate_pct
    from {{ ref('stg_fulfillment_items') }}
    group by fulfillment_id
),

locations as (
    select location_id, location_name
    from {{ ref('stg_locations') }}
),

final as (
    select
        f.fulfillment_id,
        f.store_order_id,
        f.warehouse_location_id,
        l.location_name                             as warehouse_name,
        f.assigned_to,
        f.assigned_to_name                          as picker_name,
        f.created_at                                as order_received_at,
        f.started_at,
        f.completed_at,
        f.hours_to_fulfill,
        f.status                                    as fulfillment_status,
        coalesce(i.total_items, 0)                  as total_items,
        coalesce(i.total_qty_requested, 0)          as total_qty_requested,
        coalesce(i.total_qty_picked, 0)             as total_qty_picked,
        coalesce(i.shorted_items, 0)                as shorted_items,
        coalesce(i.fully_picked_items, 0)           as fully_picked_items,
        coalesce(i.avg_fill_rate_pct, 0)            as avg_fill_rate_pct,
        case
            when f.started_at is not null and f.created_at is not null
                then round(
                    extract(epoch from (f.started_at - f.created_at)) / 3600.0,
                    2
                )
        end                                         as hours_to_pick_start,
        case
            when f.completed_at is not null and f.started_at is not null
                then round(
                    extract(epoch from (f.completed_at - f.started_at)) / 3600.0,
                    2
                )
        end                                         as picking_duration_hours,
        coalesce(i.total_qty_picked, 0) > 0
            or coalesce(i.total_items, 0) > 0       as has_items
    from fulfillment f
    left join items i on i.fulfillment_id = f.fulfillment_id
    left join locations l on l.location_id = f.warehouse_location_id
)

select * from final
order by order_received_at desc nulls last, fulfillment_id
