-- Every transaction item's line_total must equal (unit_price - discount) * quantity.
-- The discount column is a per-unit discount; line_total is the net line amount.
-- Returns rows on failure (dbt fails if any rows returned).

select item_id, transaction_id, product_id,
       unit_price, discount, quantity, line_total,
       ((unit_price - discount) * quantity)::numeric(14, 4) as expected_line_total
from {{ ref('stg_pos_transaction_items') }}
where abs(line_total - ((unit_price - discount) * quantity)) > 0.01
