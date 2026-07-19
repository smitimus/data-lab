-- Fleet utilization per truck — total loads, transit hours, on-time rate, and
-- estimated labor cost (driver hourly_rate * hours_in_transit).
-- Grain: one row per truck.
--
-- Cost note (data-lab#30): estimated_labor_cost is a LABOR-ONLY proxy. Verisim carries
-- no fuel / distance / cost columns, so cost-per-mile is intentionally omitted.

with trucks as (
    select * from {{ ref('stg_transport_trucks') }}
),

loads as (
    select * from {{ ref('stg_transport_loads') }}
),

fleet_cost as (
    select
        truck_id,
        sum(estimated_labor_cost) as total_estimated_labor_cost,
        avg(hourly_rate)          as avg_driver_hourly_rate
    from {{ ref('int_transport_enriched') }}
    group by truck_id
),

aggregated as (
    select
        t.truck_id,
        t.license_plate,
        t.make,
        t.model,
        t.year,
        t.capacity_pallets,
        t.is_active,
        count(l.load_id)                                                                        as total_loads,
        count(l.load_id) filter (where l.arrived_at is not null and l.departed_at is not null) as completed_loads,
        count(l.load_id) filter (where l.status = 'cancelled')                                 as cancelled_loads,
        sum(l.hours_in_transit)                                                                 as total_hours_in_transit,
        avg(l.hours_in_transit)                                                                 as avg_hours_per_load,
        round(
            count(l.load_id) filter (
                where l.arrived_at is not null and l.departed_at is not null
                and l.arrived_at <= l.departed_at + interval '24 hours'
            ) * 100.0 / nullif(count(l.load_id), 0),
            1
        )                                                                                       as on_time_rate_pct,
        min(l.load_date)                                                                        as first_load_date,
        max(l.load_date)                                                                        as last_load_date
    from trucks t
    left join loads l on l.truck_id = t.truck_id
    group by t.truck_id, t.license_plate, t.make, t.model, t.year,
             t.capacity_pallets, t.is_active
),

final as (
    select
        a.*,
        fc.total_estimated_labor_cost,
        fc.avg_driver_hourly_rate,
        case
            when a.total_loads > 0
                then round(a.total_hours_in_transit / a.total_loads::numeric, 2)
        end                                                                                     as hours_per_load,
        case
            when a.is_active = 'true' and a.total_loads = 0
                then 'IDLE'
            when a.is_active = 'true' and a.total_loads > 0
                then 'ACTIVE'
            else 'INACTIVE'
        end                                                                                     as utilization_status
    from aggregated a
    left join fleet_cost fc on fc.truck_id = a.truck_id
)

select * from final
order by total_loads desc
