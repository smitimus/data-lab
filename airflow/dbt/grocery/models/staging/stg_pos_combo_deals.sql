with source as (
    select * from {{ source('raw_pos', 'combo_deals') }}
),

renamed as (
    select
        deal_id,
        name                                    as deal_name,
        description,
        deal_type,
        trigger_qty::int                        as trigger_qty,
        trigger_department                      as trigger_department_name,
        deal_price::numeric                     as deal_price,
        valid_from::date                        as valid_from,
        valid_until::date                       as valid_until
    from source
)

select * from renamed
