-- Intermediate transport enrichment: one row per load, joined to truck, route
-- (warehouse -> store) locations, and driver (employee) attributes.
-- Grain: one row per load_id.
-- Source: stg_transport_loads, stg_transport_trucks, stg_locations (x2), stg_employees.
--
-- Cost note (data-lab#30): Verisim carries NO fuel / distance / cost columns. The only
-- real cost signal is the driver hourly_rate (stg_employees). estimated_labor_cost below
-- is driver_rate * hours_in_transit -- a LABOR-ONLY proxy. True cost-per-mile requires
-- route distance, which is not in the source; that metric is intentionally omitted (not
-- fabricated). Extend with a rates seed + geocoded distance to add fuel cost later.

{{ config(materialized='table') }}

with loads as (
    select * from {{ ref('stg_transport_loads') }}
),

trucks as (
    select * from {{ ref('stg_transport_trucks') }}
),

warehouse as (
    select location_id, location_name as warehouse_name
    from {{ ref('stg_locations') }}
),

store as (
    select
        location_id,
        location_name as store_name,
        city as store_city,
        state as store_state
    from {{ ref('stg_locations') }}
),

drivers as (
    select
        employee_id,
        full_name as driver_name,
        hourly_rate
    from {{ ref('stg_employees') }}
),

joined as (
    select
        l.load_id,
        l.truck_id,
        t.license_plate,
        t.make,
        t.model,
        t.capacity_pallets,
        t.is_active as truck_is_active,
        l.driver_id,
        d.driver_name,
        d.hourly_rate,
        l.warehouse_location_id,
        w.warehouse_name,
        l.destination_location_id,
        s.store_name,
        s.store_city,
        s.store_state,
        (l.warehouse_location_id || '->' || l.destination_location_id) as route_key,
        l.departed_at,
        l.arrived_at,
        l.status,
        l.hours_in_transit,
        l.load_date,
        l.hours_in_transit * d.hourly_rate as estimated_labor_cost
    from loads l
    left join trucks t on t.truck_id = l.truck_id
    left join warehouse w on w.location_id = l.warehouse_location_id
    left join store s on s.location_id = l.destination_location_id
    left join drivers d on d.employee_id = l.driver_id
)

select * from joined
