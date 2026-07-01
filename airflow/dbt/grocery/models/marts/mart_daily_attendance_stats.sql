-- Daily attendance stats by location
-- Grain: one row per (event_date, location_id)

with attendance as (
    select * from {{ ref('mart_attendance_summary') }}
),

employees as (
    select employee_id, department, is_active
    from {{ ref('stg_employees') }}
),

aggregated as (
    select
        a.event_date,
        a.location_id,
        l.location_name,
        count(distinct a.employee_id)                               as employees_present,
        count(distinct a.employee_id)
            filter (where a.has_complete_pair)                      as employees_with_complete_pair,
        count(distinct a.employee_id)
            filter (where a.has_unpaired_events)                     as employees_with_unpaired_events,
        count(distinct a.employee_id)
            filter (where e.department is not null)                  as employees_with_dept,
        round(sum(a.total_clocked_hours)::numeric, 2)               as total_clocked_hours,
        round(sum(a.total_break_hours)::numeric, 2)                 as total_break_hours,
        round(sum(a.net_hours_worked)::numeric, 2)                  as total_net_hours,
        case
            when count(distinct a.employee_id) > 0
                then round(
                    sum(a.total_clocked_hours) / count(distinct a.employee_id),
                    2
                )
        end                                                         as avg_hours_per_employee,
        count(*) filter (where a.clock_in_count > 1)                as multi_clock_in_count,
        count(*) filter (where a.clock_out_count > 1)               as multi_clock_out_count
    from attendance a
    left join {{ ref('stg_locations') }} l
        on l.location_id = a.location_id
    left join employees e
        on e.employee_id = a.employee_id
    group by a.event_date, a.location_id, l.location_name
),

final as (
    select
        *,
        case
            when employees_present > 0
                then round(total_break_hours / nullif(employees_present, 0), 2)
        end                                                         as avg_break_hours_per_employee,
        case
            when total_clocked_hours > 0
                then round(total_break_hours / total_clocked_hours * 100, 1)
        end                                                         as break_pct_of_clocked
    from aggregated
)

select * from final
order by event_date desc, location_name
