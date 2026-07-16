{{ config(materialized='table') }}

-- MetricFlow time spine: daily date dimension for cumulative metrics and time comparisons.
-- Auto-extends 2 years past today to avoid manual maintenance.
with bounds as (
    select
        '2024-01-01'::date as start_date,
        (current_date + interval '2 years')::date as end_date
)

select
    generate_series(start_date, end_date, '1 day'::interval)::date as date_day
from bounds
