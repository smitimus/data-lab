-- Stock on hand and available quantity must never be negative.
-- Negative stock indicates a data generation or sync error.
-- Returns rows on failure (dbt fails if any rows returned).

select stock_id, product_id, location_id,
       quantity_on_hand, quantity_reserved, quantity_available
from {{ ref('stg_inv_stock_levels') }}
where quantity_on_hand < 0
   or quantity_available < 0
