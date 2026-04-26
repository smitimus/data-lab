with source as (
    select * from {{ source('raw_inv', 'receipt_items') }}
),

renamed as (
    select
        receipt_item_id,
        receipt_id,
        product_id,
        product_name,
        category,
        quantity::numeric                       as quantity,
        unit_cost::numeric                      as unit_cost,
        line_total::numeric                     as line_total
    from source
)

select * from renamed
