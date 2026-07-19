-- Shrinkage analysis by department / category / cause (corrected attribution).
-- Grain: one row per (event_date, location_id, department_id, category, shrinkage_type).
--
-- Product -> department/category mapping sourced from stg_pos_products (product catalog),
-- per data-lab#5 -- NOT transaction_items.department_id.
-- Source: int_shrinkage_attributed (data-lab#27).

{{
    config(
        materialized='table'
    )
}}

with attributed as (
    select * from {{ ref('int_shrinkage_attributed') }}
),

aggregated as (
    select
        event_date,
        location_id,
        location_name,
        location_type,
        department_id,
        department_name,
        category,
        shrinkage_type,
        count(*)                            as event_count,
        sum(quantity_lost)                 as total_quantity_lost,
        sum(estimated_value_lost)          as total_value_lost
    from attributed
    group by
        event_date, location_id, location_name, location_type,
        department_id, department_name, category, shrinkage_type
)

select
    a.*,
    sum(a.total_value_lost) over (
        partition by a.event_date, a.location_id, a.department_id
    )::numeric(14,2)                                       as dept_value_lost,
    case
        when sum(a.total_value_lost) over (
            partition by a.event_date, a.location_id, a.department_id
        ) > 0
        then round(
            a.total_value_lost / nullif(sum(a.total_value_lost) over (
                partition by a.event_date, a.location_id, a.department_id
            ), 0) * 100, 2)
        else 0
    end                                                      as pct_of_dept_value_lost
from aggregated a
order by event_date desc, total_value_lost desc
