with source as (
    select * from {{ source('raw_pos', 'transaction_items') }}
),

products as (
    select product_id, department_id
    from {{ ref('stg_pos_products') }}
),

renamed as (
    select
        s.item_id,
        s.transaction_id,
        s.product_id,
        p.department_id,
        s.product_name,
        s.category,
        s.location_id,
        s.quantity::numeric                             as quantity,
        s.unit_price::numeric                           as unit_price,
        s.discount::numeric                             as discount,
        (s.unit_price::numeric - s.discount::numeric) * s.quantity::numeric as line_total,
        s.transaction_dt::timestamptz                   as transaction_dt
    from source s
    left join products p on p.product_id = s.product_id
)

select * from renamed
