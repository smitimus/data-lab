-- Route efficiency by warehouse -> store route.
-- Grain: one row per route_key (warehouse_location_id -> destination_location_id).
--
-- Cost note: estimated_route_cost_labor = SUM(driver hourly_rate * hours_in_transit).
-- No distance / fuel data in source; cost-per-mile is intentionally omitted (not fabricated).

{{ config(materialized='table') }}

with enriched as (
    select * from {{ ref('int_transport_enriched') }}
),

aggregated as (
    select
        route_key,
        warehouse_location_id,
        warehouse_name,
        destination_location_id,
        store_name,
        store_city,
        store_state,
        count(*)                                                            as total_loads,
        count(*) filter (where status = 'delivered')                        as delivered_loads,
        count(*) filter (where status in ('cancelled', 'cancelled_missing_stock'))
                                                                            as cancelled_loads,
        avg(hours_in_transit)                                              as avg_transit_hours,
        sum(hours_in_transit)                                              as total_transit_hours,
        sum(estimated_labor_cost)                                          as estimated_route_cost_labor,
        round(
            count(*) filter (
                where arrived_at is not null and departed_at is not null
                and arrived_at <= departed_at + interval '24 hours'
            ) * 100.0 / nullif(count(*), 0),
            1
        )                                                                   as on_time_rate_pct,
        round(
            count(*) filter (where status = 'delivered') * 100.0 / nullif(count(*), 0),
            1
        )                                                                   as completion_rate_pct
    from enriched
    group by
        route_key, warehouse_location_id, warehouse_name, destination_location_id,
        store_name, store_city, store_state
)

select * from aggregated
order by total_loads desc
