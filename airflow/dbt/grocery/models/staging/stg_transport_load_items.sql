with source as (
    select * from {{ source('raw_transport', 'load_items') }}
),

renamed as (
    select
        item_id,
        load_id,
        fulfillment_id,
        store_order_id
    from source
)

select * from renamed
