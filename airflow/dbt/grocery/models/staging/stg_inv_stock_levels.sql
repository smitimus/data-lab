with stock as (
    select * from {{ source('raw_inv', 'stock_levels') }}
),

pos_products as (
    select product_id, sku
    from {{ source('raw_pos', 'products') }}
),

inv_products as (
    select product_id, supplier_name
    from {{ source('raw_inv', 'products') }}
),

joined as (
    select
        s.stock_id,
        s.product_id,
        pp.sku,
        s.product_name,
        s.category,
        s.location_id,
        s.quantity_on_hand::int                             as quantity_on_hand,
        s.quantity_reserved::int                            as quantity_reserved,
        s.quantity_on_hand::int - s.quantity_reserved::int  as quantity_available,
        s.reorder_point::int                                as reorder_point,
        s.reorder_qty::int                                  as reorder_qty,
        ip.supplier_name,
        s.quantity_on_hand::int <= s.reorder_point::int     as is_below_reorder,
        s.last_updated::timestamptz                         as last_updated,
        s._sdc_extracted_at                                 as _extracted_at
    from stock s
    left join pos_products pp on pp.product_id = s.product_id
    left join inv_products ip  on ip.product_id = s.product_id
)

select * from joined
