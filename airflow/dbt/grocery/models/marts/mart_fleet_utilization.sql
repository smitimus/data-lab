-- Fleet utilization per truck — total loads, transit hours, on-time rate
-- Grain: one row per truck

with trucks as (
    select * from {{ ref('stg_transport_trucks') }}
),

loads as (
    select * from {{ ref('stg_transport_loads') }}
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
        count(l.load_id)                                        as total_loads,
        count(l.load_id) filter (where l.arrived_at is not null and l.departed_at is not null) as completed_loads,
        count(l.load_id) filter (where l.status = 'cancelled') as cancelled_loads,
        sum(l.hours_in_transit)                                 as total_hours_in_transit,
        avg(l.hours_in_transit)                                 as avg_hours_per_load,
        round(
            count(l.load_id) filter (where l.arrived_at is not null and l.departed_at is not null and l.arrived_at <= l.departed_at + interval '24 hours')
            * 100.0 / nullif(count(l.load_id), 0),
            1
        ) as on_time_rate_pct,
        min(l.load_date)                                        as first_load_date,
        max(l.load_date)                                        as last_load_date
    from trucks t
    left join loads l on l.truck_id = t.truck_id
    group by t.truck_id, t.license_plate, t.make, t.model, t.year,
             t.capacity_pallets, t.is_active
),

final as (
    select
        *,
        case
            when total_loads > 0
                then round(total_hours_in_transit / total_loads::numeric, 2)
        end                                                     as hours_per_load,
        case
            when is_active = 'true' and total_loads = 0
                then 'IDLE'
            when is_active = 'true' and total_loads > 0
                then 'ACTIVE'
            else 'INACTIVE'
        end                                                     as utilization_status
    from aggregated
)

select * from final
order by total_loads desc
