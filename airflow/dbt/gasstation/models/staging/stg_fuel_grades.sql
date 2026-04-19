with source as (
    select * from {{ source('raw_fuel', 'grades') }}
),

renamed as (
    select
        grade_id,
        name                as grade_name,
        octane_rating,
        current_price       as price_per_gallon,
        is_active,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
