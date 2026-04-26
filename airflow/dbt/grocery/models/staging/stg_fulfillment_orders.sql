with source as (
    select * from {{ source('raw_fulfillment', 'orders') }}
),

renamed as (
    select
        fulfillment_id,
        store_order_id,
        warehouse_location_id,
        assigned_to,
        assigned_to_name,
        status,
        started_at::timestamptz                 as started_at,
        completed_at::timestamptz               as completed_at,
        case
            when started_at is not null and completed_at is not null
                then extract(epoch from (completed_at::timestamptz - started_at::timestamptz)) / 3600.0
        end                                     as hours_to_fulfill,
        created_at::timestamptz                 as created_at
    from source
)

select * from renamed
