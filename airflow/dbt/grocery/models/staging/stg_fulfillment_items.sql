with source as (
    select * from {{ source('raw_fulfillment', 'items') }}
),

renamed as (
    select
        item_id,
        fulfillment_id,
        product_id,
        quantity_requested::int                 as quantity_requested,
        quantity_picked::int                    as quantity_picked,
        pick_status,
        round(
            quantity_picked::numeric / nullif(quantity_requested::numeric, 0) * 100,
            1
        )                                       as fill_rate_pct
    from source
)

select * from renamed
