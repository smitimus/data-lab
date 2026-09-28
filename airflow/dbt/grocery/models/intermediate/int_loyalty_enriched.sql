-- Intermediate loyalty enrichment: one row per (member_id, activity_date).
-- activity_date is derived from point-transaction created_at::date (the only reliable
-- per-member activity event we have; member signup_date is a single attribute, not a
-- daily series). Each row is one point-earn/redeem event joined to its POS transaction
-- (for monetary value) and to the member (for tier / points balance).
-- Grain: one row per (member_id, activity_date, pt_id) -- an event grain.
-- Source: stg_pos_loyalty_members, stg_pos_loyalty_point_transactions, stg_pos_transactions.
--
-- t_56298217: the point_txns join is an INNER join. It used to be a left join,
-- which emitted one all-null row for every member with no point event (1684 of
-- 54044 rows on the 2026-09-21 dev slot) -- rows that have no activity_date and
-- contradict the event grain documented above. The defect was invisible because
-- this model's schema entry was the block the marts.yml duplicate `data_tests`
-- key silently dropped, so no test ran against it. Both consumers
-- (mart_loyalty_rfm, mart_loyalty_engagement) build `activity` from this model
-- but start from the member list and coalesce, so removing the padding rows is
-- a no-op for them: verified per member over the same base table (max
-- activity_date, count(distinct activity_date), sum(points_earned),
-- sum(points_redeemed), count(*) filter (where tier_changed) -- 0 differences,
-- snapshot_date unchanged).
--
-- RFM note: recency/frequency/monetary are computed in mart_loyalty_rfm by aggregating
-- this event stream back up to member level.

{{ config(materialized='table') }}

with members as (
    select * from {{ ref('stg_pos_loyalty_members') }}
),

point_txns as (
    select * from {{ ref('stg_pos_loyalty_point_transactions') }}
),

transactions as (
    select
        transaction_id,
        member_id,
        transaction_date,
        total
    from {{ ref('stg_pos_transactions') }}
),

joined as (
    select
        m.member_id,
        m.tier                                            as current_tier,
        m.points_balance                                  as current_points_balance,
        m.signup_date,
        pt.pt_id,
        pt.transaction_id,
        pt.points_earned,
        pt.points_redeemed,
        pt.reason,
        pt.tier_changed,
        pt.created_at::date                               as activity_date,
        pt.created_at::timestamptz                        as activity_ts,
        t.transaction_date,
        t.total                                           as transaction_total
    from members m
    join point_txns pt on pt.member_id = m.member_id
    left join transactions t on t.transaction_id = pt.transaction_id
)

select * from joined
