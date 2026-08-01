-- Fleet cost by truck and driver.
-- Grain: one row per (truck_id, driver_id) combination that actually ran loads.
--
-- Updated (data-lab#47 / Verisim#13): adds route_cost alongside estimated_labor_cost.
-- route_cost = distance_miles * cost_per_mile (Verisim config: 1.85).
-- estimated_labor_cost = driver hourly_rate * hours_in_transit (retained for transparency).

{{ config(materialized='table') }}

with enriched as (
    select * from {{ ref('int_transport_enriched') }}
),

aggregated as (
    select
        truck_id,
        license_plate,
        make,
        model,
        capacity_pallets,
        truck_is_active,
        driver_id,
        driver_name,
        hourly_rate,
        count(*)                                                            as total_loads,
        count(*) filter (where status = 'delivered')                        as delivered_loads,
        sum(hours_in_transit)                                              as total_hours_in_transit,
        sum(estimated_labor_cost)                                          as estimated_labor_cost,
        sum(distance_miles)                                                as total_distance_miles,
        sum(route_cost)                                                    as total_route_cost,
        round(avg(hours_in_transit), 2)                                    as avg_hours_per_load
    from enriched
    group by
        truck_id, license_plate, make, model, capacity_pallets, truck_is_active,
        driver_id, driver_name, hourly_rate
)

select * from aggregated
order by total_route_cost desc nulls last
