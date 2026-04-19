with source as (
    select * from {{ source('raw_transport', 'loads') }}
),

renamed as (
    select
        load_id,
        truck_id,
        driver_id,
        warehouse_location_id,
        destination_location_id,
        departed_at,
        arrived_at,
        status,
        case
            when departed_at is not null and arrived_at is not null
                then extract(epoch from (arrived_at - departed_at)) / 3600.0
        end                                     as hours_in_transit,
        date_trunc('day', coalesce(arrived_at, departed_at))::date as load_date,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
