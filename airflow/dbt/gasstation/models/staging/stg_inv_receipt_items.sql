with source as (
    select * from {{ source('raw_inv', 'receipt_items') }}
),

pos_products as (
    select product_id, name as product_name, category, sku
    from {{ source('raw_pos', 'products') }}
),

joined as (
    select
        ri.receipt_item_id,
        ri.receipt_id,
        ri.product_id,
        p.sku,
        p.product_name,
        p.category,
        ri.quantity,
        ri.unit_cost,
        ri.line_total,
        ri._sdc_extracted_at                     as _extracted_at
    from source ri
    left join pos_products p on p.product_id = ri.product_id
)

select * from joined
