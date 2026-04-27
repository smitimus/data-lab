-- For each member's loyalty point transactions in chronological order,
-- balance_after must equal previous balance_after + points_earned - points_redeemed.
-- Returns rows on failure (dbt fails if any rows returned).
--
-- NOTE: pt_id is gen_random_uuid() — NOT insertion order. We join the parent
-- transaction to get transaction_dt for reliable ordering.

with ordered as (
    select
        lpt.pt_id,
        lpt.member_id,
        t.transaction_dt,
        lpt.points_earned,
        lpt.points_redeemed,
        lpt.points_balance_after,
        lag(lpt.points_balance_after) over (
            partition by lpt.member_id
            -- tiebreaker: when two transactions share the same transaction_dt (same
            -- backfill tick), the one with the lower balance_after was processed first
            order by t.transaction_dt, lpt.points_balance_after
        ) as prev_balance
    from {{ ref('stg_pos_loyalty_point_transactions') }} lpt
    join {{ ref('stg_pos_transactions') }} t on t.transaction_id = lpt.transaction_id
)

select pt_id, member_id, transaction_dt,
       points_earned, points_redeemed, points_balance_after,
       prev_balance,
       (prev_balance + points_earned - points_redeemed) as expected_balance
from ordered
where prev_balance is not null
  and abs(points_balance_after - (prev_balance + points_earned - points_redeemed)) > 0
