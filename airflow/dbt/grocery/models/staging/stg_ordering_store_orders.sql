with source as (
    select * from {{ source('raw_ordering', 'store_orders') }}
),

renamed as (
    select
        order_id,
        store_location_id,
        warehouse_location_id,
        created_by,
        order_dt,
        date_trunc('day', order_dt)::date       as order_date,
        requested_delivery_dt,
        approved_by,
        approved_dt,
        status,
        notes,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
