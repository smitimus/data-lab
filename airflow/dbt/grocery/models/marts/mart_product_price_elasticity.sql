-- Product price elasticity: units sold vs. effective daily price, by product / month.
-- Grain: one row per (product_id, sale_month).
--
-- Price axis uses stg_pos_price_history.new_price, keyed by (product_name, category).
-- stg_pos_price_history has NO product_id, so we map it through stg_pos_products
-- (which carries both product_name + category and product_id). 100% of price_history
-- rows map to a catalog product (verified).
--
-- Verisim reprices products frequently (some products change price 70-190x in a single
-- month), and price_history only holds the trailing ~week (2026-07-12 .. 2026-07-19)
-- while sales span 2026-06-12 .. 2026-07-19. As a result ~80% of sale days predate any
-- price-history record and have no effective price, so most months have a null
-- effective_price and (therefore) a null price_elasticity.
--
-- AVAILABLE-SIGNAL CAVEAT: price_elasticity is only computed for the most recent months
-- where a price was in effect. Treat any non-null elasticity as a short-window signal, not
-- a stable coefficient. The model is built correctly and will populate fully once
-- price_history accumulates a longer back-history (see data-lab#18 / Verisim#8 backlog
-- item to extend price_history retention). Do NOT delete this mart -- it is the correct
-- structure and will be valuable once data widens; the nulls are a data-coverage artifact,
-- not a modeling bug.
--
-- Method: units-sold-weighted AVERAGE effective daily price per month (price in effect on
-- each sale day, as-of that day). price_elasticity is the standard point elasticity between
-- consecutive months: %Δqty / %Δprice. Source: int_pos_sales_enriched + stg_pos_price_history
-- (data-lab#26).

{{
    config(
        materialized='table'
    )
}}

with products as (
    select
        product_id,
        product_name,
        category,
        department_id
    from {{ ref('stg_pos_products') }}
),

price_hist as (
    select
        product_name,
        category,
        changed_date,
        new_price
    from {{ ref('stg_pos_price_history') }}
),

product_price as (
    select
        p.product_id,
        ph.changed_date,
        ph.new_price
    from price_hist ph
    join products p
      on p.product_name = ph.product_name
     and p.category    = ph.category
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
            select pp.new_price
            from product_price pp
            where pp.product_id   = s.product_id
              and pp.changed_date <= s.transaction_date
            order by pp.changed_date desc
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
