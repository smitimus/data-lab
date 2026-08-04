-- Coupon + combo deal redemption analysis against real POS data.
-- Grain: one row per promotion (coupon_id or deal_id).
--
-- Updated (data-lab#47 / Verisim#14): per-promotion attribution now works via
-- stg_pos_transaction_items.coupon_id / deal_id. When those columns are populated,
-- true per-promotion redemption COUNT and SAVINGS are computed from item-level
-- joins. When NULL (old data seeded pre-fix), falls back to catalog uses_count.
--
-- POS-aggregated savings (coupon_total_savings, combo_total_savings) remain
-- available as type-level cross-joined constants for backward compat.

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

-- Per-coupon redemption from txn_items (when coupon_id is populated)
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

-- Per-deal redemption from txn_items (when deal_id is populated)
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

-- Type-level fallback (for rows where coupon_id/deal_id is NULL)
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
