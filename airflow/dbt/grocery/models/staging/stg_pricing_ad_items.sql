with source as (
    select * from {{ source('raw_pricing', 'ad_items') }}
),

renamed as (
    select
        ad_item_id,
        ad_id,
        product_id,
        promoted_price,
        discount_pct,
        created_at,
        _sdc_extracted_at                   as _extracted_at
    from source
)

select * from renamed
