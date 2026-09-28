-- Coupon / combo-deal attribution (t_f0bdffaf). coupon_id / deal_id were
-- hardcoded NULL here until the promotion windows upstream agreed with the
-- usage (t_01b4fe4f): the ids were always present and FK-valid on the API
-- payload, but a date assert over them was red on source data, and surfacing
-- the ids turned a vacuous pass into a real one. This is a straight
-- pass-through — same column names and types as the source, so a value change,
-- not a schema change. Downstream, mart_promotion_redemption switches from its
-- type-level fallback rollup to per-promotion attribution; it is a table, so it
-- has to be re-run for the ids to reach it.
with source as (
    select * from {{ source('raw_pos', 'transaction_items') }}
),

products as (
    select product_id, department_id
    from {{ ref('stg_pos_products') }}
),

renamed as (
    select
        s.item_id,
        s.transaction_id,
        s.product_id,
        p.department_id,
        s.product_name,
        s.category,
        s.location_id,
        s.quantity::numeric                             as quantity,
        s.unit_price::numeric                           as unit_price,
        s.discount::numeric                             as discount,
        (s.unit_price::numeric - s.discount::numeric) * s.quantity::numeric as line_total,
        s.transaction_dt::timestamptz                   as transaction_dt,
        s.coupon_id::text                               as coupon_id,
        s.deal_id::text                                 as deal_id
    from source s
    left join products p on p.product_id = s.product_id
)

select * from renamed
