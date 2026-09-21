{{ config(severity='error') }}

-- Online order reconciliation (t_24fae529)
-- Invariants proven by the generator, guarded here for schema drift:
--   1. total == subtotal + service_fee + tax (± 1 cent)
--   2. delivery orders carry the service fee; pickup orders carry none
--   3. completed_dt is set iff status = 'completed' (no_show/cancelled keep NULL)
--   4. completed orders have a final lifecycle event (picked_up or delivered)
-- Returns offending order_ids; a passing test returns 0 rows.

with final_events as (
    select order_id
    from {{ ref('stg_online_order_events') }}
    where event_type in ('picked_up', 'delivered')
    group by order_id
)

select o.order_id, 'total_mismatch' as failure_mode
from {{ ref('stg_online_orders') }} o
where abs(o.total - (o.subtotal + o.service_fee + o.tax)) > 0.01

union all

select o.order_id, 'fee_rule_violated'
from {{ ref('stg_online_orders') }} o
where (o.fulfillment_type = 'delivery' and o.service_fee = 0)
   or (o.fulfillment_type = 'pickup' and o.service_fee <> 0)

union all

select o.order_id, 'completion_dt_inconsistent'
from {{ ref('stg_online_orders') }} o
where (o.status = 'completed' and o.completed_dt is null)
   or (o.status <> 'completed' and o.completed_dt is not null)

union all

select o.order_id, 'completed_without_final_event'
from {{ ref('stg_online_orders') }} o
left join final_events fe on fe.order_id = o.order_id
where o.status = 'completed'
  and fe.order_id is null
