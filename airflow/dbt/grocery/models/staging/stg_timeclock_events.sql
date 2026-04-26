with source as (
    select * from {{ source('raw_timeclock', 'events') }}
),

renamed as (
    select
        event_id,
        employee_id,
        location_id,
        event_type,
        event_dt::timestamptz                   as event_dt,
        event_dt::date                          as event_date,
        notes,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
