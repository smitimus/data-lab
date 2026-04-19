-- Combo deals applied to transaction items must have been active at transaction time.
-- Catches deals used before valid_from or after valid_until.
-- Returns rows on failure (dbt fails if any rows returned).

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
  and (t.transaction_dt < cd.valid_from or t.transaction_dt > cd.valid_until)
