with source as (
    select * from {{ source('raw_online', 'order_events') }}
),

renamed as (
    select
        event_id::uuid                           as event_id,
        order_id::uuid                           as order_id,
        event_type,
        event_dt::timestamptz                    as event_dt,
        note,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
