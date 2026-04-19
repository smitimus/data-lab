with source as (
    select * from {{ source('raw_inv', 'receipt_items') }}
),

renamed as (
    select
        receipt_item_id,
        receipt_id,
        product_id,
        quantity,
        unit_cost,
        line_total
    from source
)

select * from renamed
