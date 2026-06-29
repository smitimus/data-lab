-- Weekly consolidated store performance KPI snapshot.
-- Grain: one row per (report_week_start, location_id)
--
-- Consolidates revenue, transactions, labor cost, shrink, inventory alerts,
-- and order activity into a single weekly ops view.

with weeks as (
    -- Generate weekly date boundaries from the transaction date range
    select distinct
        date_trunc('week', transaction_date)::date as week_start
    from {{ ref('stg_pos_transactions') }}
),

store_locs as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
    where location_type = 'store'
),

-- Revenue & transactions
store_sales as (
    select
        date_trunc('week', transaction_date)::date  as week_start,
        location_id,
        count(distinct transaction_id)              as transaction_count,
        sum(total)                                  as total_revenue,
        avg(total)                                  as avg_transaction_value,
        count(distinct transaction_date)            as days_with_sales,
        count(distinct member_id) filter (
            where member_id is not null
        )                                           as loyalty_transaction_count
    from {{ ref('stg_pos_transactions') }}
    group by 1, 2
),

-- Shrinkage
store_shrink as (
    select
        date_trunc('week', event_date)::date        as week_start,
        location_id,
        count(*)                                    as shrink_event_count,
        sum(quantity_lost)                          as total_qty_lost,
        sum(estimated_value_lost)                   as total_value_lost
    from {{ ref('stg_inv_shrinkage_events') }}
    group by 1, 2
),

-- Inventory alerts
store_inventory as (
    select
        -- Cross-join weeks with locations to get a snapshot per week
        w.week_start,
        sl.location_id,
        count(*) filter (where slv.is_below_reorder)
            as below_reorder_count,
        count(*) filter (where slv.quantity_on_hand = 0)
            as out_of_stock_count
    from weeks w
    cross join store_locs sl
    left join {{ ref('stg_inv_stock_levels') }} slv
        on  slv.location_id = sl.location_id
        -- Use latest stock snapshot available before each week end
        and slv.last_updated <= (w.week_start + interval '7 days')
    group by w.week_start, sl.location_id
),

-- Orders placed
store_orders as (
    select
        date_trunc('week', order_date)::date        as week_start,
        store_location_id                           as location_id,
        count(*)                                    as orders_placed,
        count(*) filter (where status = 'delivered')
            as orders_delivered
    from {{ ref('stg_ordering_store_orders') }}
    group by 1, 2
),

-- Labor cost
store_labor as (
    select
        date_trunc('week', report_date)::date       as week_start,
        location_id,
        sum(actual_hours)                           as total_labor_hours,
        sum(actual_cost)                            as total_labor_cost
    from {{ ref('mart_department_labor') }}
    group by 1, 2
)

select
    w.week_start,
    sl.location_id,
    sl.location_name,
    sl.city,
    sl.state,
    -- Revenue
    coalesce(ss.transaction_count, 0)               as transaction_count,
    coalesce(ss.total_revenue, 0)                   as total_revenue,
    coalesce(ss.avg_transaction_value, 0)           as avg_transaction_value,
    coalesce(ss.days_with_sales, 0)                 as days_with_sales,
    coalesce(ss.loyalty_transaction_count, 0)       as loyalty_transaction_count,
    -- Shrink
    coalesce(sk.shrink_event_count, 0)              as shrink_event_count,
    coalesce(sk.total_qty_lost, 0)                  as shrink_qty_lost,
    coalesce(sk.total_value_lost, 0)                as shrink_value_lost,
    -- Inventory health
    coalesce(si.below_reorder_count, 0)             as below_reorder_count,
    coalesce(si.out_of_stock_count, 0)              as out_of_stock_count,
    -- Supply chain
    coalesce(so.orders_placed, 0)                   as orders_placed,
    coalesce(so.orders_delivered, 0)                as orders_delivered,
    -- Labor
    coalesce(la.total_labor_hours, 0)               as total_labor_hours,
    coalesce(la.total_labor_cost, 0)                as total_labor_cost,
    -- Derived metrics
    case
        when coalesce(la.total_labor_cost, 0) > 0
        then round(100.0 * coalesce(la.total_labor_cost, 0)
            / nullif(coalesce(ss.total_revenue, 0), 0), 2)
    end                                              as labor_cost_pct_of_revenue,
    case
        when coalesce(ss.total_revenue, 0) > 0
        then round(100.0 * coalesce(sk.total_value_lost, 0)
            / nullif(coalesce(ss.total_revenue, 0), 0), 2)
    end                                              as shrink_pct_of_revenue
from weeks w
cross join store_locs sl
left join store_sales ss      on ss.week_start = w.week_start and ss.location_id = sl.location_id
left join store_shrink sk     on sk.week_start = w.week_start and sk.location_id = sl.location_id
left join store_inventory si  on si.week_start = w.week_start and si.location_id = sl.location_id
left join store_orders so     on so.week_start = w.week_start and so.location_id = sl.location_id
left join store_labor la      on la.week_start = w.week_start and la.location_id = sl.location_id
order by w.week_start desc, sl.location_name
