with source as (
    select * from {{ source('raw_inv', 'receipts') }}
),

renamed as (
    select
        receipt_id,
        location_id,
        received_by,
        received_dt,
        date_trunc('day', received_dt)::date    as received_date,
        supplier_name,
        po_number,
        total_cost,
        _sdc_extracted_at                        as _extracted_at
    from source
)

select * from renamed
