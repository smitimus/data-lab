with items as (
    select * from {{ source('raw_pos', 'transaction_items') }}
),

products as (
    select product_id, category, subcategory
    from {{ source('raw_pos', 'products') }}
),

joined as (
    select
        i.item_id,
        i.transaction_id,
        i.product_id,
        p.category,
        p.subcategory,
        i.quantity,
        i.unit_price,
        i.discount,
        i.discount > 0                              as is_discounted,
        i.line_total,
        i._sdc_extracted_at                      as _extracted_at
    from items i
    left join products p on p.product_id = i.product_id
)

select * from joined
