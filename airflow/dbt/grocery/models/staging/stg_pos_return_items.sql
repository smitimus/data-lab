with source as (
    select * from {{ source('raw_pos', 'return_items') }}
),

-- ids stay TEXT (see stg_online_orders for why).
renamed as (
    select
        return_item_id::text                     as return_item_id,
        return_id::text                          as return_id,
        transaction_item_id::text                as transaction_item_id,
        product_id::text                         as product_id,
        quantity::numeric                        as quantity,
        refund_amount::numeric(10,2)             as refund_amount,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
