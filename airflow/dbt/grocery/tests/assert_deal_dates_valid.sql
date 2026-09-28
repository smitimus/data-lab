-- Combo deals applied to a transaction item must have been inside their
-- validity window on the CALENDAR DAY of the transaction. Catches deals used
-- before valid_from or after valid_until. Returns rows on failure (dbt fails if
-- any row is returned).
--
-- ENABLED (t_f0bdffaf). Both preconditions of t_77692c68 are met:
--   1. stg_pos_transaction_items.deal_id carries the source ids (it was
--      hardcoded `null::text` before, which made the join below match nothing
--      and the assertion pass vacuously — worse than being disabled).
--   2. raw_pos.combo_deals.valid_from/valid_until describe the window the deal
--      was actually usable in (t_01b4fe4f). Before the fix, 7,079 of 10,870
--      deal-attributed items sat outside their deal's window.
--      valid_until on the deals is a forward horizon (today + 7) and is
--      expected to be: all recorded usage is at or before today.
--
-- Comparison is by calendar day (`::date`): the pre-t_7c8a145 version compared
-- transaction_dt (timestamptz) to valid_until (date), which promotes the date to
-- midnight and flags every purchase made on the last valid day.
--
-- Known coverage edge: as with coupons, raw_pos.combo_deals is loaded from an
-- `active_only` route (SOURCE_PARTIAL), so a deal that has since been
-- deactivated drops out of this join instead of being checked. The
-- relationships test on stg_pos_transaction_items.deal_id is the soft guard.

select
    ti.item_id,
    ti.transaction_id,
    ti.deal_id,
    t.transaction_dt,
    cd.valid_from,
    cd.valid_until
from {{ ref('stg_pos_transaction_items') }} ti
join {{ ref('stg_pos_transactions') }}      t  on t.transaction_id = ti.transaction_id
join {{ ref('stg_pos_combo_deals') }}       cd on cd.deal_id       = ti.deal_id
where ti.deal_id is not null
  and t.transaction_dt::date not between cd.valid_from and cd.valid_until
