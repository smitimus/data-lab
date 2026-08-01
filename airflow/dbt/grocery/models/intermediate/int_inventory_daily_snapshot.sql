-- Daily inventory snapshot per (product_id, location_id, snapshot_date).
-- Grain: one row per (product_id, location_id, snapshot_date).
--
-- Components (all real event tables; NO fabricated adjustments source):
--   * quantity_on_hand  = latest stock_levels.quantity_on_hand as of snapshot_date
--                         (point-in-time bookend: latest last_updated <= snapshot_date)
--   * receipts          = SUM(receipt_items.quantity) where received_date = snapshot_date
--   * shrinkage        = SUM(shrinkage_events.quantity_lost) where event_date = snapshot_date
--   * sales_units      = SUM(transaction_items.quantity) where transaction_date = snapshot_date
--   * unexplained_variance = prior_day_qoh - current_qoh + receipts - sales_units + shrinkage
--       (the residual after netting out receipts, sales, and shrinkage; see data-lab#27)
--
-- HONESTY NOTE: `unexplained_variance` is a RESIDUAL, not a causal adjustment ledger.
-- Verisim has no inventory_adjustments table (quantity_on_hand is mutated in place),
-- so anything not explained by receipts/sales/shrinkage lands here -- including cycle-
-- count corrections, unmodeled theft, returns, and rounding noise. Label it as
-- unexplained / cycle-count variance; do NOT present it as authoritative adjustments.
--
-- Partitions by location_type so store vs warehouse snapshots never conflate.

{{
    config(
        materialized='table'
    )
}}

with locations as (
    select location_id, location_name, location_type
    from {{ ref('stg_locations') }}
),

stock as (
    select
        s.product_id,
        s.location_id,
        s.quantity_on_hand,
        s.last_updated::date as as_of_date
    from {{ ref('stg_inv_stock_levels') }} s
),

-- latest stock bookend per (product, location) per day
stock_bookend as (
    select distinct
        product_id,
        location_id,
        as_of_date,
        first_value(quantity_on_hand) over (
            partition by product_id, location_id
            order by as_of_date desc
            rows between unbounded preceding and current row
        ) as qoh
    from stock
),

receipts as (
    select
        ri.product_id,
        r.location_id,
        r.received_date as snapshot_date,
        sum(ri.quantity) as receipts
    from {{ ref('stg_inv_receipt_items') }} ri
    join {{ ref('stg_inv_receipts') }} r on r.receipt_id = ri.receipt_id
    group by ri.product_id, r.location_id, r.received_date
),

shrinkage as (
    select
        product_id,
        location_id,
        event_date as snapshot_date,
        sum(quantity_lost) as shrinkage_units,
        sum(estimated_value_lost) as shrinkage_value
    from {{ ref('stg_inv_shrinkage_events') }}
    group by product_id, location_id, event_date
),

sales as (
    select
        i.product_id,
        t.location_id,
        t.transaction_date as snapshot_date,
        sum(i.quantity) as sales_units
    from {{ ref('stg_pos_transaction_items') }} i
    join {{ ref('stg_pos_transactions') }} t on t.transaction_id = i.transaction_id
    group by i.product_id, t.location_id, t.transaction_date
),

-- universe of (product, location, date)
dates as (
    select product_id, location_id, snapshot_date
    from receipts
    union
    select product_id, location_id, snapshot_date
    from shrinkage
    union
    select product_id, location_id, snapshot_date
    from sales
    union
    select product_id, location_id, as_of_date as snapshot_date
    from stock_bookend
),

joined as (
    select
        d.product_id,
        d.location_id,
        l.location_type,
        l.location_name,
        d.snapshot_date,
        coalesce(sb.qoh, 0)                          as quantity_on_hand,
        coalesce(r.receipts, 0)                      as receipts,
        coalesce(sh.shrinkage_units, 0)              as shrinkage_units,
        coalesce(sh.shrinkage_value, 0)              as shrinkage_value,
        coalesce(sa.sales_units, 0)                  as sales_units
    from dates d
    join locations l on l.location_id = d.location_id
    left join stock_bookend sb
        on sb.product_id = d.product_id
       and sb.location_id = d.location_id
       and sb.as_of_date = d.snapshot_date
    left join receipts r
        on r.product_id = d.product_id and r.location_id = d.location_id
       and r.snapshot_date = d.snapshot_date
    left join shrinkage sh
        on sh.product_id = d.product_id and sh.location_id = d.location_id
       and sh.snapshot_date = d.snapshot_date
    left join sales sa
        on sa.product_id = d.product_id and sa.location_id = d.location_id
       and sa.snapshot_date = d.snapshot_date
),

with_prev as (
    select
        *,
        lag(quantity_on_hand) over (
            partition by product_id, location_id order by snapshot_date
        ) as prev_quantity_on_hand
    from joined
)

select
    product_id,
    location_id,
    location_type,
    location_name,
    snapshot_date,
    quantity_on_hand,
    receipts,
    shrinkage_units,
    shrinkage_value,
    sales_units,
    coalesce(
        prev_quantity_on_hand - quantity_on_hand + receipts - sales_units + shrinkage_units,
        0
    ) as unexplained_variance
from with_prev
order by snapshot_date, location_id, product_id
