{{ config(severity='error') }}

-- Returns reconciliation (t_2382c671)
-- Every return must reference a real, EARLIER transaction, refund at most the
-- transaction total, sum of item refunds must not exceed the header refund,
-- and per line: SUM(returned quantity) <= sold quantity.
-- Returns offending rows; a passing test returns 0 rows.

with return_totals as (
    select
        r.return_id,
        r.transaction_id,
        r.return_dt,
        r.refund_amount,
        coalesce(sum(ri.quantity), 0)       as returned_qty,
        coalesce(sum(ri.refund_amount), 0)  as items_refund_sum
    from {{ ref('stg_pos_returns') }} r
    left join {{ ref('stg_pos_return_items') }} ri
           on ri.return_id = r.return_id
    group by 1, 2, 3, 4
),

line_over_return as (
    select
        ri.transaction_item_id,
        sum(ri.quantity) as returned_qty,
        max(ti.quantity) as sold_qty
    from {{ ref('stg_pos_return_items') }} ri
    join {{ ref('stg_pos_transaction_items') }} ti
      on ti.item_id = ri.transaction_item_id
    group by 1
    having sum(ri.quantity) > max(ti.quantity) + 0.001
)

select
    rt.return_id,
    rt.transaction_id,
    case
        when t.transaction_id is null                       then 'orphan_transaction'
        when rt.return_dt < t.transaction_dt                then 'return_before_sale'
        when rt.refund_amount > t.total + 0.01              then 'refund_exceeds_total'
        when rt.items_refund_sum > rt.refund_amount + 0.01  then 'items_exceed_header'
    end as failure_mode
from return_totals rt
left join {{ ref('stg_pos_transactions') }} t
       on t.transaction_id = rt.transaction_id
where t.transaction_id is null
   or rt.return_dt < t.transaction_dt
   or rt.refund_amount > t.total + 0.01
   or rt.items_refund_sum > rt.refund_amount + 0.01

union all

select
    r.return_id,
    r.transaction_id,
    'returned_more_than_sold'
from line_over_return lor
join {{ ref('stg_pos_return_items') }} ri on ri.transaction_item_id = lor.transaction_item_id
join {{ ref('stg_pos_returns') }} r on r.return_id = ri.return_id
