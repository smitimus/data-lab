with source as (
    select * from {{ source('raw_pos', 'price_history') }}
),

products as (
    select product_id, name as product_name from {{ source('raw_pos', 'products') }}
),

renamed as (
    select
        s.price_history_id,
        p.product_id,
        s.product_name,
        s.old_price::numeric                      as old_price,
        s.new_price::numeric                      as new_price,
        s.changed_at::timestamptz                 as changed_at,
        s.changed_at::date                        as changed_date
    from source s
    left join products p on s.product_name = p.product_name
)

select * from renamed
