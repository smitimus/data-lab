with source as (
    select * from {{ source('raw_pricing', 'weekly_ads') }}
),

renamed as (
    select
        ad_id,
        ad_name,
        start_date,
        end_date,
        created_at,
        _sdc_extracted_at                   as _extracted_at
    from source
)

select * from renamed
