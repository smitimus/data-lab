-- Order cycle time: order→fulfillment, fulfillment→delivery, and total
-- order→delivery duration by store / warehouse.
-- Grain: one row per store_order_id (mirrors int_fulfillment_enriched).
--
-- Durations carried from int_fulfillment_enriched (data-lab#29):
--   order_to_fulfill_hours, fulfill_to_delivery_hours, total_order_to_delivery_hours.
--
-- Source: int_fulfillment_enriched (data-lab#29).

{{
    config(
        materialized='table'
    )
}}

with enriched as (
    select * from {{ ref('int_fulfillment_enriched') }}
)

select
    order_id,
    order_date,
    store_location_id,
    store_name,
    warehouse_location_id,
    warehouse_name,
    order_status,
    fulfillment_status,
    load_status,
    days_order_to_fulfill,
    days_order_to_delivery,
    hours_to_approve,
    hours_to_fulfill,
    hours_in_transit,
    -- order → fulfillment (hours)
    case
        when fulfillment_completed_at is not null and order_date is not null
            then round(extract(epoch from (fulfillment_completed_at - order_date::timestamptz)) / 3600.0, 1)
    end                                         as order_to_fulfill_hours,
    -- fulfillment → delivery (hours)
    case
        when arrived_at is not null and fulfillment_completed_at is not null
            then round(extract(epoch from (arrived_at - fulfillment_completed_at)) / 3600.0, 1)
    end                                         as fulfill_to_delivery_hours,
    -- total order → delivery (hours)
    case
        when arrived_at is not null and order_date is not null
            then round(extract(epoch from (arrived_at - order_date::timestamptz)) / 3600.0, 1)
    end                                         as total_order_to_delivery_hours,
    on_time,
    days_late,
    requested_delivery_dt
from enriched
order by order_date desc, order_id
