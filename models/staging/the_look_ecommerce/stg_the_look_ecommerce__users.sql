with 

source as (

    select * from {{ source('the_look_ecommerce', 'users') }}

),

renamed as (

    select
        id,
        first_name,
        last_name,
        email,
        age,
        gender,
        state,
        street_address,
        postal_code,
        city,
        country,
        latitude,
        longitude,
        traffic_source,
        created_at,
        user_geom

    from source

)

select * from renamed
