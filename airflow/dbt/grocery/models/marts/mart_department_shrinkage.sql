-- Department-level shrinkage vs. revenue: shrink value, shrink % of revenue, by day/location/dept.
-- Combines mart_department_performance (revenue) with shrinkage_events (loss).

with dept_revenue as (
    select
        transaction_date,
        location_id,
        location_name,
        department_id,
        department_name,
        units_sold,
        gross_revenue,
        net_revenue
    from {{ ref('mart_department_performance') }}
),

shrinkage as (
    select * from {{ ref('stg_inv_shrinkage_events') }}
),

products as (
    select product_id, department_id
    from {{ ref('stg_pos_transaction_items') }}
    -- get distinct product → department mapping via transaction items
    -- (reuse the department_id already joined there)
),

-- Aggregate shrinkage to department level per day per location
dept_shrink as (
    select
        s.event_date,
        s.location_id,
        ti.department_id,
        s.shrinkage_type,
        count(*)                                        as shrink_events,
        sum(s.quantity_lost)                            as total_qty_lost,
        sum(s.estimated_value_lost)::numeric(12, 2)     as total_value_lost
    from shrinkage s
    left join (
        select distinct product_id, department_id
        from {{ ref('stg_pos_transaction_items') }}
    ) ti on ti.product_id = s.product_id
    group by s.event_date, s.location_id, ti.department_id, s.shrinkage_type
),

-- Roll up shrink to dept/day/location (across all types)
dept_shrink_total as (
    select
        event_date,
        location_id,
        department_id,
        sum(shrink_events)                              as total_shrink_events,
        sum(total_qty_lost)                             as total_qty_lost,
        sum(total_value_lost)::numeric(12, 2)           as total_value_lost,
        sum(total_value_lost) filter (where shrinkage_type = 'expired')::numeric(12,2)   as expired_value_lost,
        sum(total_value_lost) filter (where shrinkage_type = 'spoilage')::numeric(12,2)  as spoilage_value_lost,
        sum(total_value_lost) filter (where shrinkage_type = 'damaged')::numeric(12,2)   as damaged_value_lost,
        sum(total_value_lost) filter (where shrinkage_type = 'theft')::numeric(12,2)     as theft_value_lost
    from dept_shrink
    group by event_date, location_id, department_id
),

final as (
    select
        coalesce(r.transaction_date, ds.event_date)     as report_date,
        coalesce(r.location_id, ds.location_id)         as location_id,
        coalesce(r.location_name, l.location_name)      as location_name,
        coalesce(r.department_id, ds.department_id)     as department_id,
        coalesce(r.department_name, d.department_name)  as department_name,
        coalesce(r.net_revenue, 0)::numeric(14, 2)      as net_revenue,
        coalesce(r.units_sold, 0)                       as units_sold,
        coalesce(ds.total_shrink_events, 0)             as shrink_event_count,
        coalesce(ds.total_qty_lost, 0)                  as total_qty_lost,
        coalesce(ds.total_value_lost, 0)::numeric(12,2) as total_value_lost,
        coalesce(ds.expired_value_lost, 0)              as expired_value_lost,
        coalesce(ds.spoilage_value_lost, 0)             as spoilage_value_lost,
        coalesce(ds.damaged_value_lost, 0)              as damaged_value_lost,
        coalesce(ds.theft_value_lost, 0)                as theft_value_lost,
        case
            when coalesce(r.net_revenue, 0) > 0
            then round((ds.total_value_lost / r.net_revenue * 100)::numeric, 3)
        end                                             as shrink_pct_of_revenue
    from dept_revenue r
    full outer join dept_shrink_total ds
        on  ds.event_date     = r.transaction_date
        and ds.location_id    = r.location_id
        and ds.department_id  = r.department_id
    left join {{ ref('stg_locations') }}   l on l.location_id   = coalesce(r.location_id, ds.location_id)
    left join {{ ref('stg_pos_departments') }} d on d.department_id = coalesce(r.department_id, ds.department_id)
)

select * from final
order by report_date desc, total_value_lost desc
