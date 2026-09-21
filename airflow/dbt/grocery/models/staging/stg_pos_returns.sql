with source as (
    select * from {{ source('raw_pos', 'returns') }}
),

-- ids stay TEXT (see stg_online_orders for why).
renamed as (
    select
        return_id::text                          as return_id,
        transaction_id::text                     as transaction_id,
        location_id::text                        as location_id,
        nullif(member_id, '')::text              as member_id,
        return_dt::timestamptz                   as return_dt,
        return_dt::date                          as return_date,
        reason                                   as return_reason,
        refund_method,
        refund_amount::numeric(10,2)             as refund_amount,
        is_restocked::boolean                    as is_restocked,
        scenario_tag,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
