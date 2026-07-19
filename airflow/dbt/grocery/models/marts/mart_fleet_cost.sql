-- Fleet cost by truck and driver.
-- Grain: one row per (truck_id, driver_id) combination that actually ran loads.
--
-- estimated_labor_cost = SUM(driver hourly_rate * hours_in_transit) on that truck/driver.
-- No distance / fuel data in source; cost-per-mile intentionally omitted (not fabricated).

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
        round(avg(hours_in_transit), 2)                                    as avg_hours_per_load
    from enriched
    group by
        truck_id, license_plate, make, model, capacity_pallets, truck_is_active,
        driver_id, driver_name, hourly_rate
)

select * from aggregated
order by estimated_labor_cost desc
