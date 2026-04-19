with source as (
    select * from {{ source('raw_pos', 'loyalty_point_transactions') }}
),

renamed as (
    select
        pt_id,
        member_id,
        transaction_id,
        points_earned,
        points_redeemed,
        reason,
        balance_after                           as points_balance_after,
        reason = 'tier_upgrade'                 as tier_changed,
        created_at,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
