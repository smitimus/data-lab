with source as (
    select * from {{ source('raw_inv', 'receipts') }}
),

renamed as (
    select
        receipt_id,
        location_id,
        received_dt::timestamptz                as received_dt,
        received_dt::date                       as received_date,
        supplier_name,
        po_number,
        total_cost::numeric                     as total_cost,
        line_items::int                         as line_items,
        _sdc_extracted_at                       as _extracted_at
    from source
)

select * from renamed
