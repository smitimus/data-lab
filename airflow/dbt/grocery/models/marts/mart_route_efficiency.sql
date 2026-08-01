-- Route efficiency by warehouse -> store route.
-- Grain: one row per route_key (warehouse_location_id -> destination_location_id).
--
-- Updated (data-lab#47 / Verisim#13): now includes real cost-per-mile and route_cost.
-- cost_per_mile = sum(route_cost) / sum(distance_miles) for the route.
-- route_cost = distance_miles * 1.85 (configurable via dbt var transport_cost_per_mile).
-- estimated_route_cost_labor = SUM(driver hourly_rate * hours_in_transit) retained.

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
        sum(distance_miles)                                                as total_distance_miles,
        sum(route_cost)                                                    as total_route_cost,
        case
            when sum(distance_miles) > 0
            then round(sum(route_cost) / sum(distance_miles), 2)
            else null
        end                                                                as avg_cost_per_mile,
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
