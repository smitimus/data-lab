with source as (
    select * from {{ source('raw_pricing', 'weekly_ads') }}
),

renamed as (
    select
        ad_id,
        ad_name,
        start_date::date                    as start_date,
        end_date::date                      as end_date,
        created_at::timestamptz             as created_at,
        _sdc_extracted_at                   as _extracted_at
    from source
)

select * from renamed
