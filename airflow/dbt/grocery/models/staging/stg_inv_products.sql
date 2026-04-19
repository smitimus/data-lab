with inv_products as (
    select * from {{ source('raw_inv', 'products') }}
),

pos_products as (
    select
        product_id,
        name        as product_name,
        category,
        sku
    from {{ source('raw_pos', 'products') }}
),

joined as (
    select
        ip.inv_product_id,
        ip.product_id,
        p.product_name,
        p.category,
        p.sku,
        ip.reorder_point,
        ip.reorder_qty,
        ip.unit_of_measure,
        ip.supplier_name,
        ip.lead_time_days
    from inv_products ip
    left join pos_products p on p.product_id = ip.product_id
)

select * from joined
