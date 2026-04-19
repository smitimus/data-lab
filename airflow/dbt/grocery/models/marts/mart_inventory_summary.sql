-- Current stock per product per location with last receipt info.
-- One row per product per location.

with stock as (
    select * from {{ ref('stg_inv_stock_levels') }}
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
),

last_receipt as (
    select
        ri.product_id,
        r.location_id,
        max(r.received_dt)      as last_receipt_dt,
        sum(ri.quantity)        as total_units_received
    from {{ ref('stg_inv_receipt_items') }} ri
    join {{ ref('stg_inv_receipts') }} r on r.receipt_id = ri.receipt_id
    group by ri.product_id, r.location_id
),

final as (
    select
        s.stock_id,
        s.product_id,
        s.sku,
        s.product_name,
        s.category,
        s.location_id,
        l.location_name,
        l.city,
        l.state,
        s.quantity_on_hand,
        s.quantity_reserved,
        s.quantity_available,
        s.reorder_point,
        s.reorder_qty,
        s.supplier_name,
        s.is_below_reorder,
        case
            when s.quantity_on_hand = 0     then 'OUT_OF_STOCK'
            when s.is_below_reorder         then 'REORDER'
            else 'OK'
        end                     as stock_status,
        lr.last_receipt_dt,
        lr.total_units_received,
        s.last_updated
    from stock s
    left join locations l   on l.location_id = s.location_id
    left join last_receipt lr
        on  lr.product_id  = s.product_id
        and lr.location_id = s.location_id
)

select * from final
