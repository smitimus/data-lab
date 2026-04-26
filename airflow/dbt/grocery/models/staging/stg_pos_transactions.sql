with source as (
    select * from {{ source('raw_pos', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        location_id,
        employee_id,
        member_id,
        transaction_dt::timestamptz                                     as transaction_dt,
        transaction_dt::date                                            as transaction_date,
        date_trunc('hour', transaction_dt::timestamptz)                 as transaction_hour,
        extract(dow from transaction_dt::timestamptz)::int              as day_of_week,
        extract(hour from transaction_dt::timestamptz)::int             as hour_of_day,
        subtotal::numeric                                               as subtotal,
        coupon_savings::numeric                                         as coupon_savings,
        deal_savings::numeric                                           as deal_savings,
        tax::numeric                                                    as tax,
        total::numeric                                                  as total,
        payment_method,
        scenario_tag,
        member_id is not null                                           as has_loyalty_member,
        coupon_savings::numeric > 0                                     as has_coupon,
        deal_savings::numeric > 0                                       as has_deal,
        _sdc_extracted_at                                               as _extracted_at
    from source
)

select * from renamed
