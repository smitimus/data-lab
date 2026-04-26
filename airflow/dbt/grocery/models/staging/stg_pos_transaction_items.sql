with source as (
    select * from {{ source('raw_pos', 'transaction_items') }}
),

renamed as (
    select
        item_id,
        transaction_id,
        product_id,
        product_name,
        category,
        location_id,
        quantity::int                           as quantity,
        unit_price::numeric                     as unit_price,
        discount::numeric                       as discount,
        (unit_price::numeric - discount::numeric) * quantity::int   as line_total,
        transaction_dt::timestamptz             as transaction_dt
    from source
)

select * from renamed
