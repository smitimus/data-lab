with source as (
    select * from {{ source('raw_inv', 'stock_levels') }}
),

pos_products as (
    select product_id, name as product_name, category, sku
    from {{ source('raw_pos', 'products') }}
),

inv_products as (
    select product_id, reorder_point, reorder_qty, supplier_name
    from {{ source('raw_inv', 'products') }}
),

joined as (
    select
        s.stock_id,
        s.product_id,
        p.sku,
        p.product_name,
        p.category,
        s.location_id,
        s.quantity_on_hand,
        s.quantity_reserved,
        s.quantity_on_hand - s.quantity_reserved    as quantity_available,
        ip.reorder_point,
        ip.reorder_qty,
        ip.supplier_name,
        s.quantity_on_hand <= ip.reorder_point       as is_below_reorder,
        s.last_updated,
        s._sdc_extracted_at                          as _extracted_at
    from source s
    left join pos_products p on p.product_id = s.product_id
    left join inv_products ip on ip.product_id = s.product_id
)

select * from joined
