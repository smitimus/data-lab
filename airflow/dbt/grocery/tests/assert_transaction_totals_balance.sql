-- Every transaction total must equal subtotal + tax - coupon_savings - deal_savings.
-- The discount fields represent reductions applied before the final total.
-- Returns rows on failure (dbt fails if any rows returned).

select transaction_id, subtotal, tax, coupon_savings, deal_savings, total,
       (subtotal + tax - coupon_savings - deal_savings) as expected_total
from {{ ref('stg_pos_transactions') }}
where abs(total - (subtotal + tax - coupon_savings - deal_savings)) > 0.01
