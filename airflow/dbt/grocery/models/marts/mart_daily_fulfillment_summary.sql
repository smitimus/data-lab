-- Daily fulfillment operations summary by warehouse
-- Grain: one row per (report_date, warehouse_location_id)

with fulfillment as (
    select * from {{ ref('stg_fulfillment_orders') }}
),

items as (
    select
        fulfillment_id,
        count(*)                                    as item_count,
        sum(case when pick_status = 'picked' then 1 else 0 end) as picked_items,
        sum(case when pick_status = 'short' then 1 else 0 end)  as shorted_items,
        sum(quantity_requested)                     as qty_requested,
        sum(quantity_picked)                        as qty_picked
    from {{ ref('stg_fulfillment_items') }}
    group by fulfillment_id
),

daily as (
    select
        coalesce(f.started_at, f.created_at)::date  as report_date,
        f.warehouse_location_id,
        count(distinct f.fulfillment_id)            as total_orders,
        count(distinct f.fulfillment_id)
            filter (where f.status = 'completed')   as completed_orders,
        count(distinct f.fulfillment_id)
            filter (where f.status = 'cancelled')   as cancelled_orders,
        count(distinct f.fulfillment_id)
            filter (where f.status = 'in_progress') as in_progress_orders,
        count(distinct f.assigned_to)               as pickers_active,
        coalesce(sum(i.item_count), 0)              as total_items_picked,
        coalesce(sum(i.picked_items), 0)            as fully_picked_lines,
        coalesce(sum(i.shorted_items), 0)           as shorted_lines,
        coalesce(sum(i.qty_requested), 0)           as total_qty_requested,
        coalesce(sum(i.qty_picked), 0)              as total_qty_picked,
        coalesce(sum(i.qty_picked)::numeric / nullif(sum(i.qty_requested), 0) * 100, 0)
            as daily_fill_rate_pct,
        round(
            avg(f.hours_to_fulfill) filter (where f.hours_to_fulfill is not null),
            2
        )                                           as avg_hours_to_fulfill
    from fulfillment f
    left join items i on i.fulfillment_id = f.fulfillment_id
    group by 1, 2
),

locations as (
    select location_id, location_name
    from {{ ref('stg_locations') }}
)

select
    d.report_date,
    d.warehouse_location_id,
    l.location_name                                 as warehouse_name,
    d.total_orders,
    d.completed_orders,
    d.cancelled_orders,
    d.in_progress_orders,
    d.pickers_active,
    d.total_items_picked,
    d.fully_picked_lines,
    d.shorted_lines,
    d.total_qty_requested,
    d.total_qty_picked,
    round(d.daily_fill_rate_pct, 1)                 as daily_fill_rate_pct,
    d.avg_hours_to_fulfill,
    case
        when d.total_orders > 0
            then round(d.completed_orders * 100.0 / d.total_orders, 1)
    end                                             as completion_rate_pct,
    case
        when d.total_items_picked > 0
            then round(d.shorted_lines * 100.0 / d.total_items_picked, 1)
    end                                             as short_rate_pct
from daily d
left join locations l on l.location_id = d.warehouse_location_id
order by d.report_date desc, d.warehouse_location_id
