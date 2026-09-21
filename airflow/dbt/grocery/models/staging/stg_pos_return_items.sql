with source as (
    select * from {{ source('raw_pos', 'return_items') }}
),

renamed as (
    select
        return_item_id::uuid                     as return_item_id,
        return_id::uuid                          as return_id,
        transaction_item_id::uuid                as transaction_item_id,
        product_id::uuid                         as product_id,
        quantity::numeric                        as quantity,
        refund_amount::numeric(10,2)             as refund_amount,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
