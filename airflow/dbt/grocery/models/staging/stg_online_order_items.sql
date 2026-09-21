with source as (
    select * from {{ source('raw_online', 'order_items') }}
),

renamed as (
    select
        item_id::uuid                            as item_id,
        order_id::uuid                           as order_id,
        product_id::uuid                         as product_id,
        quantity::numeric                        as quantity,
        unit_price::numeric(8,2)                 as unit_price,
        line_total::numeric(10,2)                as line_total,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
