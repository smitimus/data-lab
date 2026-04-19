with source as (
    select * from {{ source('raw_ordering', 'store_order_items') }}
),

renamed as (
    select
        item_id,
        order_id,
        product_id,
        quantity_requested,
        quantity_approved,
        coalesce(quantity_approved, quantity_requested) as quantity_effective
    from source
)

select * from renamed
