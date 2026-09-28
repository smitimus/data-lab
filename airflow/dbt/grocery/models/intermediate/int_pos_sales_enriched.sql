-- Intermediate POS sales enrichment: one row per transaction_item, joined to product,
-- location, loyalty member, and transaction-level promotion flags/savings.
-- This is the single source for downstream POS marts (mart_category_sales_performance, etc.)
--
-- Promotion attribution note (data-lab#26, superseded in part by t_f0bdffaf):
-- stg_pos_transaction_items now carries coupon_id / deal_id, so per-item promo
-- attribution IS possible and mart_promotion_redemption does it directly. This
-- model deliberately keeps the TRANSACTION-level allocation below: what it
-- distributes is the transaction's coupon_savings + deal_savings, which the
-- source reports only as transaction-level dollar amounts, and the items carry
-- one promo id each rather than a per-item saving. Allocating that total
-- proportionally to each item's share of the transaction line_total, so
-- summing allocated_promo_savings across a transaction's items recovers the
-- transaction's coupon_savings + deal_savings exactly. Changing this to
-- item-level savings would be a modelled change, not a correction.

{{
    config(
        materialized='table'
    )
}}

with items as (
    select * from {{ ref('stg_pos_transaction_items') }}
),

txns as (
    select
        transaction_id,
        location_id,
        transaction_date,
        member_id,
        coupon_savings,
        deal_savings,
        has_loyalty_member,
        has_coupon,
        has_deal
    from {{ ref('stg_pos_transactions') }}
),

txn_totals as (
    select
        transaction_id,
        sum(line_total) as txn_line_total
    from items
    group by transaction_id
),

products as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        department_id,
        unit_cost,
        unit_margin,
        margin_pct
    from {{ ref('stg_pos_products') }}
),

locations as (
    select
        location_id,
        location_name,
        city,
        state,
        location_type
    from {{ ref('stg_locations') }}
),

members as (
    select
        member_id,
        tier as member_tier,
        points_balance as member_points_balance,
        signup_date as member_signup_date
    from {{ ref('stg_pos_loyalty_members') }}
),

joined as (
    select
        i.item_id,
        i.transaction_id,
        t.transaction_date,
        t.location_id,
        l.location_name,
        l.city,
        l.state,
        l.location_type,
        i.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.department_id,
        t.member_id,
        m.member_tier,
        m.member_points_balance,
        t.has_loyalty_member,
        t.has_coupon,
        t.has_deal,
        t.coupon_savings,
        t.deal_savings,
        i.quantity,
        i.unit_price,
        i.discount,
        i.line_total,
        (i.line_total - coalesce(p.unit_cost, 0) * i.quantity) as line_margin_proxy,
        case
            when tt.txn_line_total > 0
                then (t.coupon_savings + t.deal_savings)
                     * (i.line_total / tt.txn_line_total)
            else 0
        end as allocated_promo_savings
    from items i
    join txns t          on t.transaction_id = i.transaction_id
    join txn_totals tt   on tt.transaction_id = i.transaction_id
    left join products p on p.product_id = i.product_id
    left join locations l on l.location_id = t.location_id
    left join members m   on m.member_id = t.member_id
)

select * from joined
