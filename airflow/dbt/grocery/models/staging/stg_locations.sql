with source as (
    select * from {{ source('raw_hr', 'locations') }}
),

renamed as (
    select
        location_id,
        name                                    as location_name,
        address,
        city,
        state,
        zip,
        phone,
        opened_date::date                       as opened_date,
        location_type,
        store_sqft::int                         as store_sqft,
        num_aisles::int                         as num_aisles,
        is_active::boolean                      as is_active,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
