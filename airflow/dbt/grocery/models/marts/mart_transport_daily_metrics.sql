-- Daily transport ops metrics by warehouse
-- Grain: one row per (load_date, warehouse_location_id)

with loads as (
    select * from {{ ref('stg_transport_loads') }}
),

locations as (
    select location_id, location_name, location_type
    from {{ ref('stg_locations') }}
    where location_type = 'warehouse'
),

aggregated as (
    select
        l.load_date,
        l.warehouse_location_id,
        wh.location_name                                      as warehouse_name,
        count(l.load_id)                                      as total_loads,
        count(l.load_id) filter (where l.arrived_at is not null and l.departed_at is not null) as completed_loads,
        count(l.load_id) filter (where l.status = 'cancelled' or l.status = 'cancelled_missing_stock') as cancelled_loads,
        count(l.load_id) filter (where l.departed_at is null and l.status not like 'cancel%') as pending_loads,
        count(l.load_id) filter (where l.arrived_at is not null and l.departed_at is not null and l.arrived_at <= l.departed_at + interval '24 hours') as on_time_loads,
        round(avg(l.hours_in_transit), 2)                     as avg_transit_hours,
        coalesce(sum(l.hours_in_transit), 0)                  as total_transit_hours,
        min(l.departed_at)                                    as first_departure,
        max(l.arrived_at)                                     as last_arrival,
        count(distinct l.truck_id)                            as active_trucks
    from loads l
    left join locations wh on wh.location_id = l.warehouse_location_id
    group by l.load_date, l.warehouse_location_id, wh.location_name
),

final as (
    select
        *,
        case
            when completed_loads > 0
                then round(on_time_loads * 100.0 / completed_loads, 1)
        end                                                   as on_time_rate_pct,
        case
            when total_loads > 0
                then round(completed_loads * 100.0 / total_loads, 1)
        end                                                   as completion_rate_pct
    from aggregated
)

select * from final
order by load_date desc, warehouse_name
