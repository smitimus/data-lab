-- Fulfillment pick accuracy: how often are orders fully picked vs shorted?
-- Grain: one row per fulfillment_id

with items as (
    select
        fulfillment_id,
        count(*)                                    as total_line_items,
        sum(quantity_requested)                     as total_qty_requested,
        sum(quantity_picked)                        as total_qty_picked,
        count(*) filter (where pick_status = 'picked') as fully_picked_lines,
        count(*) filter (where pick_status = 'short')  as shorted_lines,
        round(
            avg(fill_rate_pct), 1
        )                                           as avg_fill_rate_pct,
        case
            when count(*) filter (where pick_status = 'short') = 0
            then true else false
        end                                         as is_perfect_order,
        -- Short severity: how badly were shorted items under-filled?
        round(
            avg(quantity_picked::numeric / nullif(quantity_requested::numeric, 0)) filter (where pick_status = 'short')
            * 100, 1
        )                                           as avg_shorted_line_fill_pct
    from {{ ref('stg_fulfillment_items') }}
    group by fulfillment_id
),

orders as (
    select
        fulfillment_id,
        status,
        assigned_to_name
    from {{ ref('stg_fulfillment_orders') }}
),

final as (
    select
        i.fulfillment_id,
        o.assigned_to_name                          as picker_name,
        o.status                                    as fulfillment_status,
        i.total_line_items,
        i.total_qty_requested,
        i.total_qty_picked,
        i.fully_picked_lines,
        i.shorted_lines,
        i.avg_fill_rate_pct,
        i.is_perfect_order,
        i.avg_shorted_line_fill_pct,
        round(
            i.total_qty_picked::numeric / nullif(i.total_qty_requested, 0) * 100,
            1
        )                                           as total_fill_rate_pct,
        case
            when i.total_line_items > 0
                then round(
                    i.fully_picked_lines::numeric / i.total_line_items * 100, 1
                )
        end                                         as perfect_line_rate_pct,
        case
            when i.total_qty_requested > i.total_qty_picked
                then i.total_qty_requested - i.total_qty_picked
        end                                         as total_qty_shorted
    from items i
    left join orders o on o.fulfillment_id = i.fulfillment_id
)

select * from final
order by total_line_items desc, fulfillment_id
