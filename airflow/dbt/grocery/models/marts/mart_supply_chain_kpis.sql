-- Supply chain KPIs: fill rate, short rate, on-time rate by warehouse / store / day.
-- Grain: one row per (report_date, warehouse_location_id, store_location_id).
--
-- Inputs from int_fulfillment_enriched (data-lab#29). No SLA/on-time flag exists in
-- source, so on_time is derived as arrived_at::date <= requested_delivery_dt.
--
-- Source: int_fulfillment_enriched (data-lab#29).

{{
    config(
        materialized='table'
    )
}}

with enriched as (
    select * from {{ ref('int_fulfillment_enriched') }}
),

agg as (
    select
        order_date                          as report_date,
        warehouse_location_id,
        store_location_id,
        count(*)                                                   as order_count,
        count(*) filter (where fulfillment_status = 'completed')   as fulfilled_count,
        count(*) filter (where load_status = 'delivered')           as delivered_count,
        count(*) filter (where on_time)                             as on_time_count,
        sum(coalesce(pick_qty_requested, 0))                        as total_qty_requested,
        sum(coalesce(pick_qty_picked, 0))                           as total_qty_picked,
        sum(coalesce(total_shorted_qty, 0))                         as total_shorted_qty
    from enriched
    group by order_date, warehouse_location_id, store_location_id
)

select
    report_date,
    warehouse_location_id,
    store_location_id,
    order_count,
    fulfilled_count,
    delivered_count,
    on_time_count,
    total_qty_requested,
    total_qty_picked,
    total_shorted_qty,
    round(coalesce(total_qty_picked, 0)::numeric
        / nullif(total_qty_requested, 0) * 100, 2)                 as fill_rate_pct,
    round(coalesce(total_shorted_qty, 0)::numeric
        / nullif(total_qty_requested, 0) * 100, 2)                 as short_rate_pct,
    round(on_time_count::numeric / nullif(delivered_count, 0) * 100, 2) as on_time_rate_pct
from agg
order by report_date desc, warehouse_location_id, store_location_id
