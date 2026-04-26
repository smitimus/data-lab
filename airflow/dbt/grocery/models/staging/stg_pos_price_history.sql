with source as (
    select * from {{ source('raw_pos', 'price_history') }}
),

renamed as (
    select
        price_history_id,
        product_name,
        category,
        old_price::numeric                      as old_price,
        new_price::numeric                      as new_price,
        changed_at::timestamptz                 as changed_at,
        changed_at::date                        as changed_date
    from source
)

select * from renamed
