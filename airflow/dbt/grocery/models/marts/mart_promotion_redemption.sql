-- Coupon + combo deal redemption analysis against real POS data.
-- Grain: one row per promotion (coupon_id or deal_id).
--
-- IMPORTANT data-modeling caveat (data-lab#26): Verisim transactions expose ONLY
-- has_coupon / has_deal flags plus coupon_savings / deal_savings dollar amounts.
-- They do NOT carry coupon_id / deal_id, and the coupon/combo catalogs carry no
-- transaction linkage. So per-coupon / per-deal transaction attribution is impossible.
--
-- Therefore:
--   * redemption_rate_pct uses the catalog uses_count / max_uses (Verisim-sourced) --
--     this is the only true per-promotion redemption signal available.
--   * coupon_txn_count / coupon_total_savings (and combo equivalents) are the REAL
--     POS-aggregated redemption volume and value, shown at the promotion-TYPE level
--     because they cannot be split per promotion. These are cross-joined constants:
--     coupon_* is populated on coupon rows and null on combo rows (and vice versa).
--     Actual per-promotion savings at the transaction level are NOT derivable; do not
--     present coupon_total_savings as attributable to any single coupon.

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

txns as (
    select
        transaction_id,
        has_coupon,
        has_deal,
        coupon_savings,
        deal_savings
    from {{ ref('stg_pos_transactions') }}
),

coupon_agg as (
    select
        count(*) filter (where has_coupon)                          as coupon_txn_count,
        coalesce(sum(coupon_savings), 0)                            as coupon_total_savings
    from txns
),

combo_agg as (
    select
        count(*) filter (where has_deal)                            as combo_txn_count,
        coalesce(sum(deal_savings), 0)                              as combo_total_savings
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
        c.valid_from,
        c.valid_until,
        c.is_active,
        ca.coupon_txn_count,
        ca.coupon_total_savings,
        null::bigint                                                as combo_txn_count,
        null::numeric                                               as combo_total_savings
    from coupons c
    cross join coupon_agg ca
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
        d.valid_from,
        d.valid_until,
        true                                                        as is_active,
        null::bigint                                                as coupon_txn_count,
        null::numeric                                               as coupon_total_savings,
        cb.combo_txn_count,
        cb.combo_total_savings
    from combos d
    cross join combo_agg cb
)

select * from coupon_rows
union all
select * from combo_rows
order by promotion_type, promotion_name
