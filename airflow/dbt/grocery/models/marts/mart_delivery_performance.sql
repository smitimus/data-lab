-- Delivery & fulfillment performance aggregated by week and warehouse.
-- Grain: one row per (report_week_start, warehouse_location_id)
--
-- Purpose: Track warehouse SLA — on-time delivery, cycle time, loading efficiency.

with orders as (
    select
        order_id,
        store_location_id,
        warehouse_location_id,
        order_date,
        status                                           as order_status
    from {{ ref('stg_ordering_store_orders') }}
),

fulfillment as (
    select
        store_order_id,
        fulfillment_id,
        status                                           as fulfillment_status,
        hours_to_fulfill,
        completed_at
    from {{ ref('stg_fulfillment_orders') }}
),

loads as (
    select
        li.store_order_id,
        tl.load_id,
        tl.status                                        as load_status,
        tl.departed_at,
        tl.arrived_at,
        tl.hours_in_transit,
        tl.load_date,
        tl.destination_location_id,
        tl.from_warehouse                                as warehouse_name,
        tl.to_store                                      as store_name
    from {{ ref('stg_transport_loads') }} tl
    join {{ ref('stg_transport_load_items') }} li
        on li.load_id = tl.load_id
),

all_locs as (
    select location_id, location_name
    from {{ ref('stg_locations') }}
)

select
    -- Week start (Monday) of the order date
    date_trunc('week', o.order_date)::date               as report_week_start,
    o.warehouse_location_id,
    max(al.location_name)                                as warehouse_name,
    -- Volume
    count(distinct o.order_id)                           as total_orders,
    count(distinct f.fulfillment_id)                     as total_fulfillments,
    count(distinct l.load_id)                            as total_loads,
    -- Fulfillment SLA
    count(distinct case
        when f.hours_to_fulfill is not null
        then f.fulfillment_id
    end)                                                 as fulfilled_count,
    count(distinct case
        when f.hours_to_fulfill <= 24
        then f.fulfillment_id
    end)                                                 as fulfilled_within_24h,
    round(
        avg(f.hours_to_fulfill) filter (where f.hours_to_fulfill is not null)
        , 1)                                             as avg_hours_to_fulfill,
    -- Transport SLA
    count(distinct case
        when l.load_status = 'delivered'
        then l.load_id
    end)                                                 as delivered_count,
    round(
        avg(l.hours_in_transit) filter (where l.hours_in_transit is not null)
        , 1)                                             as avg_hours_in_transit,
    -- On-time delivery rate (delivered < 48h after departure)
    case
        when count(distinct case
            when l.departed_at is not null then l.load_id
        end) > 0
        then round(
            100.0 * count(distinct case
                when l.hours_in_transit is not null and l.hours_in_transit <= 48
                then l.load_id
            end) / nullif(count(distinct case
                when l.departed_at is not null then l.load_id
            end), 0)
        , 1)
    end                                                  as on_time_delivery_pct,
    -- Destinations served
    count(distinct l.destination_location_id)            as stores_served,
    count(distinct l.store_name)                         as store_names
from orders o
left join all_locs al
    on al.location_id = o.warehouse_location_id
left join fulfillment f
    on f.store_order_id = o.order_id
left join loads l
    on l.store_order_id = o.order_id
group by date_trunc('week', o.order_date)::date,
         o.warehouse_location_id
order by report_week_start desc, warehouse_location_id
