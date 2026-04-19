-- Coupons applied to transaction items must have been valid at the time of the transaction.
-- Catches coupons used before valid_from or after valid_until.
-- Returns rows on failure (dbt fails if any rows returned).

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
  and (t.transaction_dt < c.valid_from or t.transaction_dt > c.valid_until)
