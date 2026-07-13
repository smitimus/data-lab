{{ config(severity='error') }}

-- E2E Revenue Consistency Test
-- Verifies that revenue totals match across all data layers.
-- Returns rows on failure (any mismatch > $1.00 tolerance).
-- A passing test returns 0 rows.

with

-- Revenue totals at each layer
layer_totals as (
    select
        'raw_pos.transactions' as layer,
        coalesce(sum(cast(total as numeric)), 0) as total_revenue
    from {{ source('raw_pos', 'transactions') }}

    union all

    select
        'staging.stg_pos_transactions',
        coalesce(sum(total), 0)
    from {{ ref('stg_pos_transactions') }}

    union all

    select
        'mart.mart_daily_revenue(pos_revenue)',
        coalesce(sum(pos_revenue), 0)
    from {{ ref('mart_daily_revenue') }}

    union all

    select
        'mart.mart_daily_revenue(total_revenue)',
        coalesce(sum(total_revenue), 0)
    from {{ ref('mart_daily_revenue') }}
),

-- Check: raw staging should match raw source (API data is raw, staging is cleaned)
raw_vs_staging as (
    select 'raw_vs_staging_total_mismatch' as check_name,
           (select total_revenue from layer_totals where layer = 'raw_pos.transactions') as expected,
           (select total_revenue from layer_totals where layer = 'staging.stg_pos_transactions') as actual,
           abs(
               (select total_revenue from layer_totals where layer = 'raw_pos.transactions') -
               (select total_revenue from layer_totals where layer = 'staging.stg_pos_transactions')
           ) as difference
    having abs(
        (select total_revenue from layer_totals where layer = 'raw_pos.transactions') -
        (select total_revenue from layer_totals where layer = 'staging.stg_pos_transactions')
    ) > 1.00
),

-- Check: mart pos_revenue should match staging total
mart_vs_staging as (
    select 'mart_vs_staging_revenue_mismatch' as check_name,
           (select total_revenue from layer_totals where layer = 'staging.stg_pos_transactions') as expected,
           (select total_revenue from layer_totals where layer = 'mart.mart_daily_revenue(pos_revenue)') as actual,
           abs(
               (select total_revenue from layer_totals where layer = 'staging.stg_pos_transactions') -
               (select total_revenue from layer_totals where layer = 'mart.mart_daily_revenue(pos_revenue)')
           ) as difference
    having abs(
        (select total_revenue from layer_totals where layer = 'staging.stg_pos_transactions') -
        (select total_revenue from layer_totals where layer = 'mart.mart_daily_revenue(pos_revenue)')
    ) > 1.00
),

-- Check: revenue formula consistency
-- total = subtotal + tax - coupon_savings - deal_savings
formula_check as (
    select 'total_formula_mismatch' as check_name,
           count(*) as bad_count
    from {{ ref('stg_pos_transactions') }}
    where abs(total - (subtotal + tax - coalesce(coupon_savings, 0) - coalesce(deal_savings, 0))) > 0.01
    having count(*) > 0
),

-- Check: line_total formula consistency
-- line_total = (unit_price - discount) * quantity
line_total_formula_check as (
    select 'line_total_formula_mismatch' as check_name,
           count(*) as bad_count
    from {{ ref('stg_pos_transaction_items') }}
    where abs(line_total - ((unit_price - coalesce(discount, 0)) * quantity)) > 0.01
    having count(*) > 0
)

-- Combine all failures
select check_name, expected, actual, difference, null::bigint as bad_count
from raw_vs_staging

union all
select check_name, expected, actual, difference, null
from mart_vs_staging

union all
select check_name, null, null, null, bad_count
from formula_check

union all
select check_name, null, null, null, bad_count
from line_total_formula_check
