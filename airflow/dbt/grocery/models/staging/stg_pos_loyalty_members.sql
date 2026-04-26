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
        signup_date::date                       as signup_date,
        points_balance::int                     as points_balance,
        tier,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
