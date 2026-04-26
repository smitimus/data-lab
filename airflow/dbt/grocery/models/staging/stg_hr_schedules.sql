with source as (
    select * from {{ source('raw_hr', 'schedules') }}
),

renamed as (
    select
        schedule_id,
        location_id,
        employee_id,
        scheduled_date::date                as scheduled_date,
        department,
        shift_start::time                   as shift_start,
        shift_end::time                     as shift_end,
        status,
        created_at::timestamptz             as created_at,
        _sdc_extracted_at                   as _extracted_at
    from source
)

select * from renamed
