with source as (
    select * from {{ source('raw_transport', 'trucks') }}
),

renamed as (
    select
        truck_id,
        license_plate,
        make,
        model,
        year,
        capacity_pallets,
        is_active
    from source
)

select * from renamed
