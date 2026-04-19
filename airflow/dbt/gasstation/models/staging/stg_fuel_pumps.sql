with source as (
    select * from {{ source('raw_fuel', 'pumps') }}
),

renamed as (
    select
        pump_id,
        location_id,
        pump_number,
        num_sides,
        is_active,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
