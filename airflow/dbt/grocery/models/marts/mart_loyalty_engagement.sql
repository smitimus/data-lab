-- Loyalty engagement summary: active rate, redemption rate, points liability,
-- tier migration. This is a single-row summary of the loyalty program (plus a per-tier
-- breakdown for tier migration).
-- Grain: one row per current_tier (with an 'ALL' rollup via the union).
--
-- Definitions (data-lab#31):
--   redemption_rate   = total points redeemed / total points earned (program-wide)
--   active_rate       = share of members active in last 90 days (activity since snapshot-90d)
--   points_liability  = sum of current points_balance across active members (outstanding obligation)
--   tier_upgrades     = count of tier_changed events (reason='tier_upgrade') in the point log
-- Source: int_loyalty_enriched, stg_pos_loyalty_members, stg_pos_transactions.

{{ config(materialized='table') }}

with members as (
    select
        member_id,
        tier,
        points_balance,
        (points_balance > 0) as is_active_member_flag
    from {{ ref('stg_pos_loyalty_members') }}
),

activity as (
    select
        member_id,
        max(activity_date) as last_activity_date,
        count(*) filter (where tier_changed) as tier_change_events
    from {{ ref('int_loyalty_enriched') }}
    group by member_id
),

reference as (
    select max(activity_date) as snapshot_date from {{ ref('int_loyalty_enriched') }}
),

totals as (
    select
        sum(points_earned)   as total_points_earned,
        sum(points_redeemed) as total_points_redeemed,
        count(*) filter (where tier_changed) as total_tier_upgrades
    from {{ ref('int_loyalty_enriched') }}
),

member_metrics as (
    select
        m.member_id,
        m.tier,
        m.points_balance,
        m.is_active_member_flag,
        a.last_activity_date,
        r.snapshot_date,
        (a.last_activity_date >= r.snapshot_date - 90) as is_active_90d
    from members m
    left join activity a on a.member_id = m.member_id
    cross join reference r
),

summary as (
    select
        count(*)                                                          as total_members,
        count(*) filter (where is_active_90d)                            as active_members_90d,
        round(
            count(*) filter (where is_active_90d) * 100.0 / nullif(count(*), 0),
            1
        )                                                                 as active_rate_pct,
        round(
            coalesce(max(t.total_points_redeemed), 0) * 100.0
            / nullif(coalesce(max(t.total_points_earned), 0), 0),
            1
        )                                                                 as redemption_rate_pct,
        coalesce(sum(m.points_balance) filter (where m.is_active_member_flag), 0) as points_liability,
        coalesce(max(t.total_tier_upgrades), 0)                          as tier_upgrades
    from member_metrics m
    cross join totals t
),

per_tier as (
    select
        m.tier as current_tier,
        count(*) as member_count,
        sum(m.points_balance) as tier_points_balance
    from members m
    group by m.tier
)

select
    'ALL' as current_tier,
    null::bigint as member_count,
    s.active_rate_pct,
    s.redemption_rate_pct,
    s.points_liability,
    s.tier_upgrades,
    s.total_members
from summary s

union all

select
    pt.current_tier,
    pt.member_count,
    null::numeric as active_rate_pct,
    null::numeric as redemption_rate_pct,
    pt.tier_points_balance as points_liability,
    null::bigint as tier_upgrades,
    null::bigint as total_members
from per_tier pt
