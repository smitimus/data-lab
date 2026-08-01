-- Loyalty program: member tier distribution, point accumulation, tier upgrade activity
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
        transaction_date    as sale_date,
        total
    from {{ ref('stg_pos_transactions') }}
    where member_id is not null
),

-- Member lifetime stats
member_stats as (
    select
        m.member_id,
        m.first_name,
        m.last_name,
        m.email,
        m.tier                                              as loyalty_tier,
        m.points_balance,
        m.signup_date,
        count(distinct t.transaction_id)                    as total_transactions,
        sum(t.total)::numeric(14,2)                         as total_spend,
        avg(t.total)::numeric(10,2)                         as avg_transaction_value,
        min(t.sale_date)                                    as first_purchase_date,
        max(t.sale_date)                                    as last_purchase_date,
        sum(pt.points_earned)                               as lifetime_points_earned,
        count(*) filter (where pt.tier_changed)             as tier_upgrades
    from members m
    left join transactions t  on t.member_id  = m.member_id
    left join point_txns  pt  on pt.member_id = m.member_id
    group by
        m.member_id, m.first_name, m.last_name, m.email,
        m.tier, m.points_balance, m.signup_date
),

-- Tier distribution summary
tier_summary as (
    select
        loyalty_tier,
        count(*)                                as member_count,
        avg(points_balance)::numeric(10,1)      as avg_points_balance,
        avg(total_spend)::numeric(12,2)         as avg_lifetime_spend,
        avg(total_transactions)::numeric(8,1)   as avg_transactions
    from member_stats
    group by loyalty_tier
)

select
    ms.*,
    ts.member_count                             as tier_total_members,
    ts.avg_lifetime_spend                       as tier_avg_spend
from member_stats ms
left join tier_summary ts on ts.loyalty_tier = ms.loyalty_tier
order by ms.points_balance desc
