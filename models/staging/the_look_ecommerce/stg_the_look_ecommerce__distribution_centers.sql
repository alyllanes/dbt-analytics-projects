with 

source as (

    select * from {{ source('the_look_ecommerce', 'distribution_centers') }}

),

renamed as (

    select
        id,
        name,
        latitude,
        longitude,
        distribution_center_geom

    from source

)

select * from renamed
