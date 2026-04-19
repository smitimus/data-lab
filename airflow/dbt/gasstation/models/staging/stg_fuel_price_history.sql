with source as (
    select * from {{ source('raw_fuel', 'price_history') }}
),

grades as (
    select grade_id, name as grade_name
    from {{ source('raw_fuel', 'grades') }}
),

joined as (
    select
        ph.price_history_id,
        ph.grade_id,
        g.grade_name,
        ph.old_price,
        ph.new_price,
        round((ph.new_price - ph.old_price)::numeric, 4)   as price_change,
        round(((ph.new_price - ph.old_price)
              / nullif(ph.old_price, 0) * 100)::numeric, 2) as price_change_pct,
        ph.changed_at,
        ph._sdc_extracted_at                     as _extracted_at
    from source ph
    left join grades g on g.grade_id = ph.grade_id
)

select * from joined
