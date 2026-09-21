with source as (
    select * from {{ source('raw_online', 'orders') }}
),

renamed as (
    select
        order_id::uuid                           as order_id,
        order_number::bigint                     as order_number,
        location_id::uuid                        as location_id,
        nullif(member_id, '')::uuid              as member_id,
        placed_dt::timestamptz                   as placed_dt,
        placed_dt::date                          as order_date,
        fulfillment_type,
        status,
        subtotal::numeric(10,2)                  as subtotal,
        service_fee::numeric(10,2)               as service_fee,
        tax::numeric(10,2)                       as tax,
        total::numeric(10,2)                     as total,
        payment_method,
        nullif(pickup_window_start, '')::timestamptz  as pickup_window_start,
        nullif(pickup_window_end, '')::timestamptz    as pickup_window_end,
        nullif(promised_delivery_dt, '')::timestamptz as promised_delivery_dt,
        nullif(completed_dt, '')::timestamptz         as completed_dt,
        (status = 'completed')                   as is_completed,
        (status = 'cancelled')                   as is_cancelled,
        (status = 'no_show')                     as is_no_show,
        scenario_tag,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
