with source as (
    select * from {{ source('raw_online', 'order_items') }}
),

-- ids stay TEXT (see stg_online_orders for why).
renamed as (
    select
        item_id::text                            as item_id,
        order_id::text                           as order_id,
        product_id::text                         as product_id,
        quantity::numeric                        as quantity,
        unit_price::numeric(8,2)                 as unit_price,
        line_total::numeric(10,2)                as line_total,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
