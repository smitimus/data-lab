with source as (
    select * from {{ source('raw_fuel', 'transactions') }}
),

grades as (
    select grade_id, name as grade_name
    from {{ source('raw_fuel', 'grades') }}
),

joined as (
    select
        t.transaction_id,
        t.pump_id,
        t.location_id,
        t.employee_id,
        t.member_id,
        t.transaction_dt,
        date_trunc('day',  t.transaction_dt)::date  as transaction_date,
        date_trunc('hour', t.transaction_dt)        as transaction_hour,
        extract(dow from t.transaction_dt)::int     as day_of_week,
        extract(hour from t.transaction_dt)::int    as hour_of_day,
        t.grade_id,
        g.grade_name,
        t.gallons,
        t.price_per_gallon,
        t.total_amount,
        t.payment_method,
        t.scenario_tag,
        t.member_id is not null                     as has_loyalty_member,
        t._sdc_extracted_at                      as _extracted_at
    from source t
    left join grades g on g.grade_id = t.grade_id
)

select * from joined
