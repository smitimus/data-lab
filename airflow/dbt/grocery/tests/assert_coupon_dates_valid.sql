-- Coupons applied to a transaction item must have been inside their validity
-- window on the CALENDAR DAY of the transaction. Catches coupons used before
-- valid_from or after valid_until. Returns rows on failure (dbt fails if any
-- row is returned).
--
-- ENABLED (t_f0bdffaf). Both preconditions of t_77692c68 are met:
--   1. stg_pos_transaction_items.coupon_id carries the source ids (it was
--      hardcoded `null::text` before, which made the join below match nothing
--      and the assertion pass vacuously — worse than being disabled).
--   2. raw_pos.coupons.valid_from/valid_until describe the window the coupon was
--      actually usable in (t_01b4fe4f: the generator seeded valid_from = the
--      load day while back-filling 30 days of transactions; every window now
--      covers its own recorded usage). Before the fix, 24,015 of 34,997
--      coupon-attributed items sat outside their coupon's window.
--
-- Comparison is by calendar day (`::date`). The pre-t_7c8a145 version compared
-- transaction_dt (timestamptz) to valid_until (date), which promotes the date to
-- midnight and flags every purchase made on the last valid day. Measured on the
-- pre-fix data the two forms agreed (24,015 rows either way) because the
-- violations were whole-month offsets, but the day form is the intended rule.
--
-- Known coverage edge: raw_pos.coupons is loaded from an `active_only` route
-- (SOURCE_PARTIAL in grocery_ingest_api), so an item whose coupon has since
-- been deactivated drops out of this join instead of being checked. The
-- relationships test on stg_pos_transaction_items.coupon_id is the soft guard
-- for that side of the pair.

select
    ti.item_id,
    ti.transaction_id,
    ti.coupon_id,
    t.transaction_dt,
    c.valid_from,
    c.valid_until
from {{ ref('stg_pos_transaction_items') }} ti
join {{ ref('stg_pos_transactions') }}     t  on t.transaction_id = ti.transaction_id
join {{ ref('stg_pos_coupons') }}          c  on c.coupon_id      = ti.coupon_id
where ti.coupon_id is not null
  and t.transaction_dt::date not between c.valid_from and c.valid_until
