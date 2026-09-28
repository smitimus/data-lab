-- Coupon + combo deal redemption analysis against real POS data.
-- Grain: one row per promotion (coupon_id or deal_id).
--
-- Per-promotion attribution (data-lab#47 / Verisim#14, live since t_f0bdffaf):
-- stg_pos_transaction_items.coupon_id / deal_id are populated, so the
-- attributed_* columns below are true per-promotion counts and totals from
-- item-level joins rather than the type-level fallback. The cross-joined
-- coupon_txn_count / coupon_total_savings / combo_* pair is kept as the
-- promotion-TYPE total across all promotions, which is what a cross-promotion
-- comparison needs; it does not depend on the ids being null.
--
-- The two families are NOT comparable term-for-term, and that is the source's
-- definition, not a defect here: has_coupon / has_deal are exactly
-- `coupon_savings > 0` / `deal_savings > 0` on the transaction, while
-- attributed_txn_count counts transactions carrying a promo-TAGGED LINE.
-- Measured at t_f0bdffaf: coupons 12,802 flagged vs 8,961 attributed; deals
-- 1,909 flagged vs 9,529 attributed — the deal gap runs the other way because
-- deal_id tags participating lines whether or not the combo condition was met
-- and savings applied.

{{
    config(
        materialized='table'
    )
}}

with coupons as (
    select * from {{ ref('stg_pos_coupons') }}
),

combos as (
    select * from {{ ref('stg_pos_combo_deals') }}
),

txn_items as (
    select
        item_id,
        transaction_id,
        coupon_id,
        deal_id,
        line_total
    from {{ ref('stg_pos_transaction_items') }}
),

txns as (
    select
        transaction_id,
        has_coupon,
        has_deal,
        coupon_savings,
        deal_savings
    from {{ ref('stg_pos_transactions') }}
),

-- Per-coupon redemption from txn_items (populated since t_f0bdffaf)
coupon_item_agg as (
    select
        ti.coupon_id,
        count(distinct ti.transaction_id)   as txn_count,
        count(distinct ti.item_id)          as item_count,
        sum(ti.line_total)                  as item_total
    from txn_items ti
    where ti.coupon_id is not null
    group by ti.coupon_id
),

-- Per-deal redemption from txn_items (populated since t_f0bdffaf)
deal_item_agg as (
    select
        ti.deal_id,
        count(distinct ti.transaction_id)   as txn_count,
        count(distinct ti.item_id)          as item_count,
        sum(ti.line_total)                  as item_total
    from txn_items ti
    where ti.deal_id is not null
    group by ti.deal_id
),

-- Promotion-TYPE totals, across every promotion of that type (see the header:
-- this is a cross-promotion constant, not the per-promotion fallback it used to
-- be read as when the ids were null)
coupon_txn_agg as (
    select
        count(*) filter (where has_coupon)   as coupon_txn_count,
        coalesce(sum(coupon_savings), 0)     as coupon_total_savings
    from txns
),

combo_txn_agg as (
    select
        count(*) filter (where has_deal)     as combo_txn_count,
        coalesce(sum(deal_savings), 0)       as combo_total_savings
    from txns
),

coupon_rows as (
    select
        'coupon'::text                                              as promotion_type,
        c.coupon_id                                                 as promotion_id,
        c.code                                                      as promotion_name,
        c.description,
        c.coupon_type                                               as promotion_detail,
        c.department_name,
        c.uses_count,
        c.max_uses,
        case when c.max_uses > 0
            then round((c.uses_count::numeric / c.max_uses * 100)::numeric, 2)
            else null
        end                                                         as redemption_rate_pct,
        -- True per-coupon redemption from txn_items (when available)
        coalesce(cia.txn_count, 0)                                  as attributed_txn_count,
        coalesce(cia.item_count, 0)                                 as attributed_item_count,
        coalesce(cia.item_total, 0)                                 as attributed_item_total,
        c.valid_from,
        c.valid_until,
        c.is_active,
        ca.coupon_txn_count,
        ca.coupon_total_savings,
        null::bigint                                                as combo_txn_count,
        null::numeric                                               as combo_total_savings
    from coupons c
    left join coupon_item_agg cia on cia.coupon_id = c.coupon_id
    cross join coupon_txn_agg ca
),

combo_rows as (
    select
        'combo_deal'::text                                         as promotion_type,
        d.deal_id                                                   as promotion_id,
        d.deal_name                                                 as promotion_name,
        d.description,
        d.deal_type                                                 as promotion_detail,
        d.trigger_department_name                                   as department_name,
        null::int                                                   as uses_count,
        null::int                                                   as max_uses,
        null::numeric                                               as redemption_rate_pct,
        -- True per-deal redemption from txn_items (when available)
        coalesce(dia.txn_count, 0)                                  as attributed_txn_count,
        coalesce(dia.item_count, 0)                                 as attributed_item_count,
        coalesce(dia.item_total, 0)                                 as attributed_item_total,
        d.valid_from,
        d.valid_until,
        true                                                        as is_active,
        null::bigint                                                as coupon_txn_count,
        null::numeric                                               as coupon_total_savings,
        cb.combo_txn_count,
        cb.combo_total_savings
    from combos d
    left join deal_item_agg dia on dia.deal_id = d.deal_id
    cross join combo_txn_agg cb
)

select * from coupon_rows
union all
select * from combo_rows
order by promotion_type, promotion_name
