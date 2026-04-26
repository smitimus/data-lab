with source as (
    select * from {{ source('raw_hr', 'employees') }}
),

renamed as (
    select
        employee_id,
        location_id,
        first_name,
        last_name,
        first_name || ' ' || last_name          as full_name,
        email,
        hire_date::date                         as hire_date,
        termination_date::date                  as termination_date,
        department,
        job_title,
        hourly_rate::numeric                    as hourly_rate,
        status,
        (termination_date is null)              as is_active,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
