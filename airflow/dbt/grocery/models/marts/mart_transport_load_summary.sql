-- Per-load transport summary: truck, driver, route, transit time, status
-- Grain: one row per load_id

with loads as (
    select * from {{ ref('stg_transport_loads') }}
),

trucks as (
    select * from {{ ref('stg_transport_trucks') }}
),

locations as (
    select location_id, location_name, location_type from {{ ref('stg_locations') }}
),

final as (
    select
        l.load_id,
        l.truck_id,
        t.license_plate,
        t.make || ' ' || t.model                         as truck_model,
        t.year                                            as truck_year,
        t.capacity_pallets,
        l.driver_id,
        l.driver,
        l.from_warehouse                                  as warehouse_name,
        wh.location_id                                    as warehouse_location_id,
        l.to_store                                        as store_name,
        st.location_id                                    as store_location_id,
        l.departed_at,
        l.arrived_at,
        l.hours_in_transit,
        l.status,
        l.load_date,
        coalesce(l.arrived_at, l.departed_at) is not null  as has_departed,
        l.arrived_at is not null                           as has_arrived,
        l.arrived_at is not null
            and l.departed_at is not null
            and l.arrived_at <= l.departed_at + interval '24 hours' as is_on_time,
        l._extracted_at
    from loads l
    left join trucks t on t.truck_id = l.truck_id
    left join locations wh on wh.location_id = l.warehouse_location_id
    left join locations st on st.location_id = l.destination_location_id
)

select * from final
order by load_date desc, departed_at desc
