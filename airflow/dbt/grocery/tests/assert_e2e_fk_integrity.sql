{{ config(severity='warn') }}

-- E2E Foreign Key Integrity Test
-- Verifies every FK relationship in the staging layer.
-- Returns one row per orphaned FK relationship when failures exist.
-- A passing test returns 0 rows.

with

-- Helper: count orphaned rows for each FK relationship
fk_checks as (
    -- employees → locations
    select 'stg_employees.location_id -> stg_locations.location_id' as fk_name,
           count(*) as orphan_count
    from {{ ref('stg_employees') }} e
    left join {{ ref('stg_locations') }} l on e.location_id = l.location_id
    where l.location_id is null

    union all
    select 'stg_hr_schedules.employee_id -> stg_employees.employee_id',
           count(*)
    from {{ ref('stg_hr_schedules') }} s
    left join {{ ref('stg_employees') }} e on s.employee_id = e.employee_id
    where e.employee_id is null

    union all
    select 'stg_hr_schedules.location_id -> stg_locations.location_id',
           count(*)
    from {{ ref('stg_hr_schedules') }} s
    left join {{ ref('stg_locations') }} l on s.location_id = l.location_id
    where l.location_id is null

    union all
    select 'stg_pos_products.department_id -> stg_pos_departments.department_id',
           count(*)
    from {{ ref('stg_pos_products') }} p
    left join {{ ref('stg_pos_departments') }} d on p.department_id = d.department_id
    where d.department_id is null

    union all
    select 'stg_pos_transactions.location_id -> stg_locations.location_id',
           count(*)
    from {{ ref('stg_pos_transactions') }} t
    left join {{ ref('stg_locations') }} l on t.location_id = l.location_id
    where l.location_id is null

    union all
    select 'stg_pos_transactions.employee_id -> stg_employees.employee_id',
           count(*)
    from {{ ref('stg_pos_transactions') }} t
    left join {{ ref('stg_employees') }} e on t.employee_id = e.employee_id
    where e.employee_id is null

    union all
    select 'stg_pos_transaction_items.transaction_id -> stg_pos_transactions.transaction_id',
           count(*)
    from {{ ref('stg_pos_transaction_items') }} ti
    left join {{ ref('stg_pos_transactions') }} t on ti.transaction_id = t.transaction_id
    where t.transaction_id is null

    union all
    select 'stg_pos_transaction_items.product_id -> stg_pos_products.product_id',
           count(*)
    from {{ ref('stg_pos_transaction_items') }} ti
    left join {{ ref('stg_pos_products') }} p on ti.product_id = p.product_id
    where p.product_id is null

    union all
    select 'stg_pos_loyalty_point_transactions.member_id -> stg_pos_loyalty_members.member_id',
           count(*)
    from {{ ref('stg_pos_loyalty_point_transactions') }} lpt
    left join {{ ref('stg_pos_loyalty_members') }} lm on lpt.member_id = lm.member_id
    where lm.member_id is null

    union all
    select 'stg_pos_loyalty_point_transactions.transaction_id -> stg_pos_transactions.transaction_id',
           count(*)
    from {{ ref('stg_pos_loyalty_point_transactions') }} lpt
    left join {{ ref('stg_pos_transactions') }} t on lpt.transaction_id = t.transaction_id
    where t.transaction_id is null

    union all
    select 'stg_timeclock_events.employee_id -> stg_employees.employee_id',
           count(*)
    from {{ ref('stg_timeclock_events') }} te
    left join {{ ref('stg_employees') }} e on te.employee_id = e.employee_id
    where e.employee_id is null

    union all
    select 'stg_timeclock_events.location_id -> stg_locations.location_id',
           count(*)
    from {{ ref('stg_timeclock_events') }} te
    left join {{ ref('stg_locations') }} l on te.location_id = l.location_id
    where l.location_id is null

    union all
    select 'stg_ordering_store_orders.store_location_id -> stg_locations.location_id',
           count(*)
    from {{ ref('stg_ordering_store_orders') }} o
    left join {{ ref('stg_locations') }} l on o.store_location_id = l.location_id
    where l.location_id is null

    union all
    select 'stg_ordering_store_order_items.order_id -> stg_ordering_store_orders.order_id',
           count(*)
    from {{ ref('stg_ordering_store_order_items') }} oi
    left join {{ ref('stg_ordering_store_orders') }} o on oi.order_id = o.order_id
    where o.order_id is null

    union all
    select 'stg_ordering_store_order_items.product_id -> stg_pos_products.product_id',
           count(*)
    from {{ ref('stg_ordering_store_order_items') }} oi
    left join {{ ref('stg_pos_products') }} p on oi.product_id = p.product_id
    where p.product_id is null

    union all
    select 'stg_fulfillment_orders.store_order_id -> stg_ordering_store_orders.order_id',
           count(*)
    from {{ ref('stg_fulfillment_orders') }} fo
    left join {{ ref('stg_ordering_store_orders') }} o on fo.store_order_id = o.order_id
    where o.order_id is null

    union all
    select 'stg_inv_stock_levels.product_id -> stg_pos_products.product_id',
           count(*)
    from {{ ref('stg_inv_stock_levels') }} sl
    left join {{ ref('stg_pos_products') }} p on sl.product_id = p.product_id
    where p.product_id is null

    union all
    select 'stg_inv_stock_levels.location_id -> stg_locations.location_id',
           count(*)
    from {{ ref('stg_inv_stock_levels') }} sl
    left join {{ ref('stg_locations') }} l on sl.location_id = l.location_id
    where l.location_id is null

    union all
    select 'stg_inv_receipts.location_id -> stg_locations.location_id',
           count(*)
    from {{ ref('stg_inv_receipts') }} r
    left join {{ ref('stg_locations') }} l on r.location_id = l.location_id
    where l.location_id is null

    union all
    select 'stg_pricing_ad_items.ad_id -> stg_pricing_weekly_ads.ad_id',
           count(*)
    from {{ ref('stg_pricing_ad_items') }} ai
    left join {{ ref('stg_pricing_weekly_ads') }} wa on ai.ad_id = wa.ad_id
    where wa.ad_id is null
)

-- Return only failing FKs
select fk_name, orphan_count
from fk_checks
where orphan_count > 0
order by fk_name
