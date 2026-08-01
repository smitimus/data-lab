-- Daily transport ops metrics by warehouse, with estimated labor cost.
-- Grain: one row per (load_date, warehouse_location_id).
--
-- Cost note (data-lab#30): estimated_labor_cost is a LABOR-ONLY proxy (driver hourly_rate
-- * hours_in_transit). Verisim carries no fuel / distance / cost columns.

with loads as (
    select * from {{ ref('stg_transport_loads') }}
),

locations as (
    select location_id, location_name, location_type
    from {{ ref('stg_locations') }}
    where location_type = 'warehouse'
),

daily_cost as (
    select
        load_date,
        warehouse_location_id,
        sum(estimated_labor_cost) as total_estimated_labor_cost
    from {{ ref('int_transport_enriched') }}
    group by load_date, warehouse_location_id
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
        a.*,
        dc.total_estimated_labor_cost,
        case
            when a.completed_loads > 0
                then round(a.on_time_loads * 100.0 / a.completed_loads, 1)
        end                                                   as on_time_rate_pct,
        case
            when a.total_loads > 0
                then round(a.completed_loads * 100.0 / a.total_loads, 1)
        end                                                   as completion_rate_pct
    from aggregated a
    left join daily_cost dc
        on dc.load_date = a.load_date
       and dc.warehouse_location_id = a.warehouse_location_id
)

select * from final
order by load_date desc, warehouse_name
