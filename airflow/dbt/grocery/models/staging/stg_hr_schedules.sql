with source as (
    select * from {{ source('raw_hr', 'schedules') }}
),

with_duration as (
    select
        schedule_id,
        location_id,
        employee_id,
        scheduled_date::date                    as scheduled_date,
        department,
        shift_start::time                       as shift_start,
        shift_end::time                         as shift_end,
        status,
        -- Correct shift duration handling midnight-crossing shifts
        -- e.g. 22:00 -> 06:00 should be 8h not -16h
        (
            extract(epoch from (shift_end::time - shift_start::time)) / 3600.0
            + case when shift_end::time <= shift_start::time then 24.0 else 0 end
        )::numeric(5, 2)                        as scheduled_hours,
        case when status = 'completed'
            then (
                extract(epoch from (shift_end::time - shift_start::time)) / 3600.0
                + case when shift_end::time <= shift_start::time then 24.0 else 0 end
            )
            else 0
        end::numeric(5, 2)                      as actual_hours,
        created_at::timestamptz                 as created_at,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from with_duration
