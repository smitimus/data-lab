-- Labor cost analysis: scheduled cost vs. actual worked cost vs. store revenue.
-- Uses hr.schedules (shift duration × hourly_rate) as the basis for cost.
-- One row per employee per day.

with schedules as (
    select
        s.schedule_id,
        s.employee_id,
        s.location_id,
        s.scheduled_date,
        s.department,
        s.shift_start,
        s.shift_end,
        s.status,
        -- shift duration in hours
        extract(epoch from (s.shift_end::time - s.shift_start::time)) / 3600.0
                                                        as scheduled_hours
    from {{ ref('stg_hr_schedules') }} s
    where s.status <> 'scheduled'   -- only resolved
),

employees as (
    select
        employee_id,
        full_name,
        job_title,
        hourly_rate,
        status
    from {{ ref('stg_employees') }}
),

locations as (
    select location_id, location_name, city, state
    from {{ ref('stg_locations') }}
),

-- Daily revenue per location (to calculate labor % of revenue)
daily_store_revenue as (
    select
        transaction_date,
        location_id,
        sum(total)::numeric(14, 2)  as daily_revenue,
        count(*)                    as daily_transactions
    from {{ ref('stg_pos_transactions') }}
    group by transaction_date, location_id
),

-- Per-employee daily scheduled cost
employee_daily as (
    select
        s.scheduled_date,
        s.employee_id,
        s.location_id,
        s.department,
        e.full_name,
        e.job_title,
        e.hourly_rate,
        e.status,
        -- scheduled
        sum(s.scheduled_hours)::numeric(8, 2)                       as scheduled_hours,
        sum(s.scheduled_hours * e.hourly_rate)::numeric(10, 2)      as scheduled_labor_cost,
        count(*)                                                     as shifts_scheduled,
        -- actuals by status
        sum(case when s.status = 'completed'  then s.scheduled_hours else 0 end)::numeric(8, 2)
                                                                     as actual_hours_worked,
        sum(case when s.status = 'completed'  then s.scheduled_hours * e.hourly_rate else 0 end)::numeric(10, 2)
                                                                     as actual_labor_cost,
        count(case when s.status = 'completed'  then 1 end)         as shifts_completed,
        count(case when s.status = 'called_out' then 1 end)         as shifts_called_out,
        count(case when s.status = 'no_show'    then 1 end)         as shifts_no_show
    from schedules s
    left join employees e on e.employee_id = s.employee_id
    group by
        s.scheduled_date, s.employee_id, s.location_id, s.department,
        e.full_name, e.job_title, e.hourly_rate, e.status
),

final as (
    select
        ed.scheduled_date,
        ed.employee_id,
        ed.full_name,
        ed.job_title,
        ed.department,
        ed.hourly_rate,
        ed.status,
        ed.location_id,
        l.location_name,
        l.city,
        l.state,
        ed.shifts_scheduled,
        ed.shifts_completed,
        ed.shifts_called_out,
        ed.shifts_no_show,
        ed.scheduled_hours,
        ed.scheduled_labor_cost,
        ed.actual_hours_worked,
        ed.actual_labor_cost,
        -- variance
        round(ed.actual_labor_cost - ed.scheduled_labor_cost, 2)    as cost_variance,
        -- labor cost % of daily store revenue
        dr.daily_revenue,
        case
            when dr.daily_revenue > 0
            then round(ed.actual_labor_cost / dr.daily_revenue * 100, 3)
        end                                                         as labor_pct_of_revenue
    from employee_daily ed
    left join locations l on l.location_id = ed.location_id
    left join daily_store_revenue dr
           on dr.location_id      = ed.location_id
          and dr.transaction_date = ed.scheduled_date
)

select * from final
order by scheduled_date desc, actual_labor_cost desc
