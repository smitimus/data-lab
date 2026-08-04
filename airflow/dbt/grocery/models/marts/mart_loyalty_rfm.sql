-- Loyalty RFM segmentation by member.
-- Grain: one row per member_id.
--
-- Recency  = days since the member's most recent activity (point event). Lower = better.
-- Frequency = count of distinct POS transactions with member_id.
-- Monetary = sum of transaction total for the member.
-- Segment = 5x5x5 score combination collapsed to a label (Champions / Loyal / At Risk /
--           Hibernating / New), with explicit cutoffs documented below.
-- Source: int_loyalty_enriched, stg_pos_transactions, stg_pos_loyalty_members.

{{ config(materialized='table') }}

with members as (
    select
        member_id,
        tier,
        points_balance,
        signup_date
    from {{ ref('stg_pos_loyalty_members') }}
),

activity as (
    select
        member_id,
        max(activity_date)                       as last_activity_date,
        count(distinct activity_date)            as active_days,
        sum(points_earned)                       as lifetime_points_earned,
        sum(points_redeemed)                     as lifetime_points_redeemed
    from {{ ref('int_loyalty_enriched') }}
    group by member_id
),

txns as (
    select
        member_id,
        count(distinct transaction_id)           as frequency,
        sum(total)                               as monetary
    from {{ ref('stg_pos_transactions') }}
    where member_id is not null
    group by member_id
),

reference as (
    select max(activity_date) as snapshot_date from {{ ref('int_loyalty_enriched') }}
),

scored as (
    select
        m.member_id,
        m.tier,
        m.points_balance,
        m.signup_date,
        a.last_activity_date,
        r.snapshot_date,
        (r.snapshot_date - a.last_activity_date) as recency_days,
        coalesce(a.active_days, 0)               as active_days,
        coalesce(t.frequency, 0)                 as frequency,
        coalesce(t.monetary, 0)::numeric(14,2)   as monetary,
        coalesce(a.lifetime_points_earned, 0)    as lifetime_points_earned,
        coalesce(a.lifetime_points_redeemed, 0)  as lifetime_points_redeemed,
        -- Recency score: 5 = most recent (<=30d), 1 = >365d
        case
            when (r.snapshot_date - a.last_activity_date) <= 30  then 5
            when (r.snapshot_date - a.last_activity_date) <= 90  then 4
            when (r.snapshot_date - a.last_activity_date) <= 180 then 3
            when (r.snapshot_date - a.last_activity_date) <= 365 then 2
            else 1
        end                                       as recency_score,
        -- Frequency score: quintile by distinct transaction count
        ntile(5) over (order by coalesce(t.frequency, 0)) as frequency_score,
        -- Monetary score: quintile by total spend
        ntile(5) over (order by coalesce(t.monetary, 0))    as monetary_score
    from members m
    left join activity a   on a.member_id   = m.member_id
    left join txns t       on t.member_id   = m.member_id
    cross join reference r
),

segmented as (
    select
        *,
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 4 and frequency_score >= 3 then 'Loyal'
            when recency_score >= 3 and frequency_score <= 2 then 'At Risk'
            when recency_score <= 2 then 'Hibernating'
            when frequency_score = 1 and recency_score >= 4 then 'New'
            else 'Regular'
        end as rfm_segment
    from scored
)

select * from segmented
order by monetary desc
