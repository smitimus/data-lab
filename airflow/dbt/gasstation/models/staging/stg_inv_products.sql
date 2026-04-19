with source as (
    select * from {{ source('raw_inv', 'products') }}
),

pos_products as (
    select product_id, name as product_name, category, subcategory, sku
    from {{ source('raw_pos', 'products') }}
),

joined as (
    select
        ip.inv_product_id,
        ip.product_id,
        p.sku,
        p.product_name,
        p.category,
        p.subcategory,
        ip.reorder_point,
        ip.reorder_qty,
        ip.unit_of_measure,
        ip.supplier_name,
        ip.lead_time_days,
        ip._sdc_extracted_at                     as _extracted_at
    from source ip
    left join pos_products p on p.product_id = ip.product_id
)

select * from joined
