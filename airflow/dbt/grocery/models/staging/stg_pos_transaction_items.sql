with transaction_items as (
    select * from {{ source('raw_pos', 'transaction_items') }}
),

products as (
    select
        product_id,
        name        as product_name,
        category,
        department_id
    from {{ source('raw_pos', 'products') }}
),

joined as (
    select
        ti.item_id,
        ti.transaction_id,
        ti.product_id,
        p.product_name,
        p.category,
        p.department_id,
        ti.quantity,
        ti.unit_price,
        ti.discount,
        ti.coupon_id,
        ti.deal_id,
        ti.line_total
    from transaction_items ti
    left join products p on p.product_id = ti.product_id
)

select * from joined
