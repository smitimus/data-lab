with source as (
    select * from {{ source('raw_pos', 'price_history') }}
),

renamed as (
    select
        price_history_id,
        product_id,
        old_price,
        new_price,
        changed_at,
        date_trunc('day', changed_at)::date     as changed_date
    from source
)

select * from renamed
