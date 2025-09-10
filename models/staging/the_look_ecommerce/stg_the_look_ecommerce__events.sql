with 

source as (

    select * from {{ source('the_look_ecommerce', 'events') }}

),

renamed as (

    select
        id,
        user_id,
        sequence_number,
        session_id,
        created_at,
        ip_address,
        city,
        state,
        postal_code,
        browser,
        traffic_source,
        uri,
        event_type

    from source

)

select * from renamed
