with source as (
    select * from {{ source('raw_pos', 'coupons') }}
),

renamed as (
    select
        coupon_id,
        code,
        description,
        coupon_type,
        discount_value,
        min_purchase,
        department_id,
        product_id,
        max_uses,
        uses_count,
        valid_from,
        valid_until,
        is_active
    from source
)

select * from renamed
