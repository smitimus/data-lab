# Data Dictionary — Cross-Domain Reporting

Maps each reporting domain to the dbt marts that power it and the business
questions those marts answer. All marts live in the `mart` schema of the
`grocery` Postgres DB and are refreshed daily by the `grocery_dbt` Airflow DAG.

## POS & Sales + Promotions (`store_pos_promotions`)
| Mart | Answers |
|---|---|
| `mart_daily_revenue` | Daily revenue, transactions, units by store/location |
| `mart_category_sales_performance` | Revenue, units, basket size by category/store/day |
| `mart_promotion_effectiveness` | Weekly-ad promo lift vs baseline |
| `mart_promotion_redemption` | Coupon/combo redemption volume & savings (type-level) |
| `mart_product_price_elasticity` | Units vs price over time (data-starved; see Verisim#15) |

## Inventory & Shrinkage (`store_inventory_shrinkage`)
| Mart | Answers |
|---|---|
| `mart_inventory_valuation` | On-hand value at cost by store/department |
| `mart_shrinkage_analysis` | Shrink $ and % by dept/category/cause |
| `mart_inventory_turnover` | Turnover + margin context per product |

## HR & Labor (`store_hr_labor`)
| Mart | Answers |
|---|---|
| `mart_labor_cost_by_department` | Scheduled/actual labor cost vs revenue by dept |
| `mart_attendance_compliance` | Late-arrival, no-show, overtime, break compliance |
| `mart_department_labor` | Attendance & hours utilization by dept |
| `mart_employee_cost` | Per-employee cost & labor % of revenue |

## Fulfillment / Supply Chain (`store_supply_chain`)
| Mart | Answers |
|---|---|
| `mart_supply_chain_kpis` | Fill rate, short rate, on-time rate |
| `mart_order_cycle_time` | Order→fulfillment→delivery durations |
| `mart_order_fulfillment_funnel` | Pipeline stage + SLA breaches |
| `mart_fulfillment_pick_accuracy` | Pick accuracy / fill rate |

## Transport & Logistics (`store_transport_fleet`)
| Mart | Answers |
|---|---|
| `mart_route_efficiency` | On-time %, est. labor cost, transit hours by route |
| `mart_fleet_cost` | Estimated labor cost by truck/driver |
| `mart_fleet_utilization` | Utilization status & on-time by truck |
| `mart_transport_daily_metrics` | Daily load volume, completion, cost |

> Cost note: transport carries no distance/fuel source data (Verisim#13). Cost
> figures are **driver labor only** (`driver_hourly_rate × hours_in_transit`).
> A true `cost_per_mile` lands after Verisim#13 + data-lab#33.

## Loyalty & CRM (`store_loyalty_crm`)
| Mart | Answers |
|---|---|
| `mart_loyalty_rfm` | Recency/frequency/monetary member segments |
| `mart_loyalty_engagement` | Active rate, redemption rate, points liability, tier upgrades |
| `mart_loyalty_cohort` | Tier distribution & member lifetime stats |

## Executive Overview (`store_performance_exec`)
Pulls the headline KPI from each domain's primary mart:
`mart_daily_revenue`, `mart_inventory_valuation`, `mart_shrinkage_analysis`,
`mart_labor_cost_by_department`, `mart_supply_chain_kpis`,
`mart_route_efficiency`, `mart_loyalty_engagement`.
