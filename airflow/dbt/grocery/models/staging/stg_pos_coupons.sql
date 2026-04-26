with source as (
    select * from {{ source('raw_pos', 'coupons') }}
),

renamed as (
    select
        coupon_id,
        code,
        description,
        coupon_type,
        discount_value::numeric                 as discount_value,
        min_purchase::numeric                   as min_purchase,
        department                              as department_name,
        max_uses::int                           as max_uses,
        uses_count::int                         as uses_count,
        valid_from::date                        as valid_from,
        valid_until::date                       as valid_until,
        is_active::boolean                      as is_active
    from source
)

select * from renamed
