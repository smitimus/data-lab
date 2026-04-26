with source as (
    select * from {{ source('raw_inv', 'shrinkage_events') }}
),

renamed as (
    select
        shrinkage_id                            as event_id,
        product_id,
        location_id,
        recorded_at::date                       as event_date,
        recorded_at::timestamptz                as recorded_at,
        reason                                  as shrinkage_type,
        quantity::numeric                       as quantity_lost,
        estimated_cost::numeric                 as estimated_value_lost,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
