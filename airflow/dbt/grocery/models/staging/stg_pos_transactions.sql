with source as (
    select * from {{ source('raw_pos', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        location_id,
        employee_id,
        member_id,
        transaction_dt,
        date_trunc('day', transaction_dt)::date as transaction_date,
        date_trunc('hour', transaction_dt)      as transaction_hour,
        extract(dow from transaction_dt)::int   as day_of_week,
        extract(hour from transaction_dt)::int  as hour_of_day,
        subtotal,
        coupon_savings,
        deal_savings,
        tax,
        total,
        payment_method,
        scenario_tag,
        member_id is not null                   as has_loyalty_member,
        coupon_savings > 0                      as has_coupon,
        deal_savings > 0                        as has_deal,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
