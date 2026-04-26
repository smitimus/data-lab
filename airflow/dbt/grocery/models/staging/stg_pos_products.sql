with source as (
    select * from {{ source('raw_pos', 'products') }}
),

departments as (
    select department_id, name as dept_name
    from {{ source('raw_pos', 'departments') }}
),

renamed as (
    select
        s.product_id,
        s.sku,
        s.upc,
        s.name                                                          as product_name,
        s.brand,
        d.department_id,
        s.department                                                    as department_name,
        s.category,
        s.subcategory,
        s.unit_size,
        s.unit_of_measure,
        s.cost::numeric                                                 as unit_cost,
        s.current_price::numeric                                        as unit_price,
        round((s.current_price::numeric - s.cost::numeric), 4)         as unit_margin,
        round(((s.current_price::numeric - s.cost::numeric)
            / nullif(s.current_price::numeric, 0) * 100), 2)           as margin_pct,
        s.is_organic::boolean                                           as is_organic,
        s.is_local::boolean                                             as is_local,
        s.is_active::boolean                                            as is_active,
        s._sdc_extracted_at                                             as _extracted_at
    from source s
    left join departments d on d.dept_name = s.department
)

select * from renamed
