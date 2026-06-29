-- Inventory turnover & stock aging by product/location.
-- Grain: one row per (product_id, location_id)
--
-- Metrics:
--   days_since_last_receipt    — how long since goods last arrived
--   units_sold_30d             — recent sales velocity
--   estimated_turnover_days    — how many days to sell current stock at recent pace
--   stock_aging_category       — healthy / slow_mover / dead_stock / out_of_stock

with stock as (
    select
        product_id,
        location_id,
        quantity_on_hand,
        quantity_available,
        reorder_point,
        is_below_reorder,
        last_updated
    from {{ ref('stg_inv_stock_levels') }}
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
    where location_type = 'store'
),

-- Latest receipt date per product per location
last_receipt as (
    select
        ri.product_id,
        r.location_id,
        max(r.received_dt) as last_receipt_dt
    from {{ ref('stg_inv_receipt_items') }} ri
    join {{ ref('stg_inv_receipts') }} r on r.receipt_id = ri.receipt_id
    group by ri.product_id, r.location_id
),

-- Sales velocity: units sold in last 30 days per product per store
recent_sales as (
    select
        ti.product_id,
        ti.location_id,
        sum(ti.quantity) as units_sold_30d,
        count(distinct date_trunc('day', ti.transaction_dt)) as days_with_sales_30d
    from {{ ref('stg_pos_transaction_items') }} ti
    where ti.transaction_dt >= current_date - interval '30 days'
    group by ti.product_id, ti.location_id
),

-- Total receipts volume for broader turnover picture
receipt_volume as (
    select
        ri.product_id,
        r.location_id,
        sum(ri.quantity)             as total_units_received,
        count(distinct r.receipt_id) as receipt_count
    from {{ ref('stg_inv_receipt_items') }} ri
    join {{ ref('stg_inv_receipts') }} r on r.receipt_id = ri.receipt_id
    where r.received_dt >= current_date - interval '60 days'
    group by ri.product_id, r.location_id
)

select
    s.product_id,
    s.location_id,
    l.location_name,
    l.city,
    l.state,
    s.quantity_on_hand,
    s.quantity_available,
    s.reorder_point,
    s.is_below_reorder,
    lr.last_receipt_dt,
    extract(day from (current_timestamp - lr.last_receipt_dt))::int
        as days_since_last_receipt,
    coalesce(rs.units_sold_30d, 0)                              as units_sold_30d,
    coalesce(rs.days_with_sales_30d, 0)                         as days_with_sales_30d,
    coalesce(rv.total_units_received, 0)                        as total_units_received_60d,
    coalesce(rv.receipt_count, 0)                               as receipt_count_60d,
    -- Estimated daily sell rate (at least 0.01 to avoid div-by-zero)
    greatest(coalesce(rs.units_sold_30d, 0) / 30.0, 0.01)
        as daily_sell_rate,
    -- Days of supply: how long current stock would last at current velocity
    case
        when s.quantity_on_hand > 0
            then round(s.quantity_on_hand / greatest(coalesce(rs.units_sold_30d, 0) / 30.0, 0.01), 1)
        else null
    end                                                         as days_of_supply,
    -- Estimated annual turnover rate (units sold / avg stock)
    case
        when s.quantity_on_hand > 0 and coalesce(rs.units_sold_30d, 0) > 0
            then round((coalesce(rs.units_sold_30d, 0) * 12.0) / nullif(s.quantity_on_hand, 0), 1)
        else 0
    end                                                         as estimated_annual_turnover,
    -- Stock aging classification
    case
        when s.quantity_on_hand = 0                             then 'OUT_OF_STOCK'
        when coalesce(rs.units_sold_30d, 0) = 0
            and s.quantity_on_hand > 0                          then 'DEAD_STOCK'
        when s.quantity_on_hand > coalesce(rs.units_sold_30d, 0) * 2
            and coalesce(rs.units_sold_30d, 0) > 0              then 'OVERSTOCKED'
        when s.is_below_reorder                                 then 'REORDER_NEEDED'
        when lr.last_receipt_dt is null
            and s.quantity_on_hand > 0                          then 'NO_RECEIPT_HISTORY'
        else 'HEALTHY'
    end                                                         as stock_aging_category,
    s.last_updated
from stock s
join locations l on l.location_id = s.location_id
left join last_receipt lr on lr.product_id = s.product_id and lr.location_id = s.location_id
left join recent_sales rs on rs.product_id = s.product_id and rs.location_id = s.location_id
left join receipt_volume rv on rv.product_id = s.product_id and rv.location_id = s.location_id
