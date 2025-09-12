{{
    config(
        schema='staging'
    )
}}

with 

source as (

    select * from {{ source('the_look_ecommerce', 'orders') }}

),

renamed as (

    select
        order_id,
        user_id,
        status,
        gender,
        created_at,
        returned_at,
        shipped_at,
        delivered_at,
        num_of_item

    from source

)

select * from renamed
