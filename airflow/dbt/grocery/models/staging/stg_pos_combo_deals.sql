with source as (
    select * from {{ source('raw_pos', 'combo_deals') }}
),

renamed as (
    select
        deal_id,
        name                                    as deal_name,
        description,
        deal_type,
        trigger_qty,
        trigger_product_id,
        trigger_department_id,
        deal_price,
        valid_from,
        valid_until,
        is_active
    from source
)

select * from renamed
