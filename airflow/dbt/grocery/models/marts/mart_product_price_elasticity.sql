-- Product price elasticity: units sold vs. effective daily price, by product / month.
-- Grain: one row per (product_id, sale_month).
--
-- Updated (data-lab#47 / Verisim#15 reseed): price_history now carries product_id
-- directly (Verisim reseed changed the schema). Joins via product_id instead of
-- the old product_name+category path. price_elasticity is the standard point
-- elasticity between consecutive months: %Δqty / %Δprice.
--
-- AVAILABLE-SIGNAL CAVEAT: early months are partial (June = 19 days in this
-- dataset, ~10 days for the reseed), so the first month's elasticity is a
-- ramp-up artifact. Treat non-null elasticity as a short-window signal until
-- 2+ full steady-state months accumulate.

{{
    config(
        materialized='table'
    )
}}

with price_hist as (
    select
        product_id,
        changed_date,
        new_price
    from {{ ref('stg_pos_price_history') }}
),

sales as (
    select
        e.product_id,
        date_trunc('month', e.transaction_date)::date as sale_month,
        e.transaction_date,
        sum(e.quantity)                                as day_units,
        sum(e.line_total)                              as day_revenue
    from {{ ref('int_pos_sales_enriched') }} e
    group by e.product_id, date_trunc('month', e.transaction_date)::date, e.transaction_date
),

daily as (
    select
        product_id,
        sale_month,
        transaction_date,
        sum(day_units)    as day_units,
        sum(day_revenue)  as day_revenue
    from sales
    group by product_id, sale_month, transaction_date
),

price_on_day as (
    select
        s.product_id,
        s.sale_month,
        s.transaction_date,
        s.day_units,
        s.day_revenue,
        (
            select ph.new_price
            from price_hist ph
            where ph.product_id   = s.product_id
              and ph.changed_date <= s.transaction_date
            order by ph.changed_date desc
            limit 1
        ) as day_price
    from daily s
),

monthly as (
    select
        product_id,
        sale_month,
        sum(day_units)                                                  as units_sold,
        sum(day_revenue)                                                as revenue,
        -- units-weighted average effective daily price for the month
        case
            when sum(day_units) > 0 and bool_or(day_price is not null)
            then sum(day_units * day_price) / sum(day_units)
            else null
        end                                                             as effective_price
    from price_on_day
    group by product_id, sale_month
),

with_lag as (
    select
        product_id,
        sale_month,
        units_sold,
        revenue,
        effective_price,
        lag(units_sold)     over w as prev_units,
        lag(effective_price) over w as prev_price
    from monthly
    window w as (partition by product_id order by sale_month)
)

select
    product_id,
    sale_month,
    units_sold,
    revenue,
    effective_price,
    prev_units,
    prev_price,
    case
        when prev_price is not null and prev_price > 0
         and prev_units is not null and prev_units > 0
         and effective_price is not null and effective_price <> prev_price
        then round(
            (((units_sold - prev_units)::numeric / prev_units)
             / ((effective_price - prev_price)::numeric / prev_price))::numeric,
            4
        )
        else null
    end as price_elasticity
from with_lag
order by product_id, sale_month
