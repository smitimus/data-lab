with source as (
    select * from {{ source('raw_pos', 'departments') }}
),

renamed as (
    select
        department_id,
        name                                    as department_name,
        code,
        is_active
    from source
)

select * from renamed
