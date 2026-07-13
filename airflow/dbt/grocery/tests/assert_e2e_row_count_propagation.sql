{{ config(severity='error') }}

-- E2E Row Count Propagation Test
-- Verifies that row counts remain consistent across raw, staging, and mart layers.
-- Each sub-query returns rows only when the assertion FAILS.
-- A passing test returns 0 rows.

with

-- Raw vs staging counts for 1:1 mappings
raw_staging_counts as (
    select 'locations' as entity,
           (select count(*) from {{ source('raw_hr', 'locations') }}) as raw_count,
           (select count(*) from {{ ref('stg_locations') }}) as staging_count
    union all
    select 'employees',
           (select count(*) from {{ source('raw_hr', 'employees') }}),
           (select count(*) from {{ ref('stg_employees') }})
    union all
    select 'departments',
           (select count(*) from {{ source('raw_pos', 'departments') }}),
           (select count(*) from {{ ref('stg_pos_departments') }})
    union all
    select 'products',
           (select count(*) from {{ source('raw_pos', 'products') }}),
           (select count(*) from {{ ref('stg_pos_products') }})
    union all
    select 'transactions',
           (select count(*) from {{ source('raw_pos', 'transactions') }}),
           (select count(*) from {{ ref('stg_pos_transactions') }})
    union all
    select 'transaction_items',
           (select count(*) from {{ source('raw_pos', 'transaction_items') }}),
           (select count(*) from {{ ref('stg_pos_transaction_items') }})
    union all
    select 'timeclock_events',
           (select count(*) from {{ source('raw_timeclock', 'events') }}),
           (select count(*) from {{ ref('stg_timeclock_events') }})
    union all
    select 'store_orders',
           (select count(*) from {{ source('raw_ordering', 'store_orders') }}),
           (select count(*) from {{ ref('stg_ordering_store_orders') }})
    union all
    select 'store_order_items',
           (select count(*) from {{ source('raw_ordering', 'store_order_items') }}),
           (select count(*) from {{ ref('stg_ordering_store_order_items') }})
    union all
    select 'fulfillment_orders',
           (select count(*) from {{ source('raw_fulfillment', 'orders') }}),
           (select count(*) from {{ ref('stg_fulfillment_orders') }})
    union all
    select 'inv_stock_levels',
           (select count(*) from {{ source('raw_inv', 'stock_levels') }}),
           (select count(*) from {{ ref('stg_inv_stock_levels') }})
    union all
    select 'pricing_weekly_ads',
           (select count(*) from {{ source('raw_pricing', 'weekly_ads') }}),
           (select count(*) from {{ ref('stg_pricing_weekly_ads') }})
),

-- Find mismatches
mismatches as (
    select entity, raw_count, staging_count,
           'raw_staging_mismatch' as failure_type
    from raw_staging_counts
    where raw_count != staging_count
),

-- Mart aggregation check: sum of mart_daily_revenue.transaction_count
-- should match total POS transactions
mart_aggregation_check as (
    select 'mart_daily_revenue_transaction_sum' as entity,
           (select count(*)::integer from {{ ref('stg_pos_transactions') }}) as expected_count,
           (select coalesce(sum(pos_transaction_count), 0)::integer from {{ ref('mart_daily_revenue') }}) as actual_count
),

filtered_mart_mismatches as (
    select entity,
           expected_count,
           actual_count,
           'mart_aggregation_mismatch' as failure_type
    from mart_aggregation_check
    where expected_count != actual_count
)

-- Combine all failures
select entity,
       raw_count::text as raw_count,
       staging_count::text as staging_count,
       null::text as expected_count,
       null::text as actual_count,
       failure_type
from mismatches

union all

select entity,
       null, null,
       expected_count::text,
       actual_count::text,
       failure_type
from filtered_mart_mismatches
