with source as (
    select * from {{ source('raw_online', 'order_events') }}
),

-- ids stay TEXT (see stg_online_orders for why).
renamed as (
    select
        event_id::text                           as event_id,
        order_id::text                           as order_id,
        event_type,
        event_dt::timestamptz                    as event_dt,
        note,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
