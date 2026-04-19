with source as (
    select * from {{ source('raw_pos', 'loyalty_members') }}
),

renamed as (
    select
        member_id,
        first_name,
        last_name,
        first_name || ' ' || last_name          as full_name,
        email,
        phone,
        signup_date,
        points_balance,
        tier,
        created_at,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
