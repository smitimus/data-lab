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
        date_trunc('day',  transaction_dt)::date    as transaction_date,
        date_trunc('hour', transaction_dt)          as transaction_hour,
        extract(dow from transaction_dt)::int       as day_of_week,      -- 0=Sun, 6=Sat
        extract(hour from transaction_dt)::int      as hour_of_day,
        subtotal,
        tax,
        total,
        payment_method,
        scenario_tag,
        member_id is not null                       as has_loyalty_member,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
