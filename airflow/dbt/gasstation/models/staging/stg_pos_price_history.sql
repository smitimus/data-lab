with source as (
    select * from {{ source('raw_pos', 'price_history') }}
),

products as (
    select product_id, name as product_name, category
    from {{ source('raw_pos', 'products') }}
),

joined as (
    select
        ph.price_history_id,
        ph.product_id,
        p.product_name,
        p.category,
        ph.old_price,
        ph.new_price,
        round((ph.new_price - ph.old_price)::numeric, 2)   as price_change,
        round(((ph.new_price - ph.old_price)
              / nullif(ph.old_price, 0) * 100)::numeric, 2) as price_change_pct,
        ph.changed_at,
        ph.changed_by,
        ph._sdc_extracted_at                     as _extracted_at
    from source ph
    left join products p on p.product_id = ph.product_id
)

select * from joined
