with source as (
    select * from {{ source('raw_pos', 'products') }}
),

renamed as (
    select
        product_id,
        sku,
        upc,
        name                                                        as product_name,
        brand,
        department_id,
        category,
        subcategory,
        unit_size,
        unit_of_measure,
        cost                                                        as unit_cost,
        current_price                                               as unit_price,
        round((current_price - cost)::numeric, 4)                  as unit_margin,
        round(((current_price - cost) / nullif(current_price, 0) * 100)::numeric, 2) as margin_pct,
        is_organic,
        is_local,
        is_active,
        _sdc_extracted_at                                           as _extracted_at
    from source
)

select * from renamed
