{{
    config(
        schema='the_look_ecommerce'
    )
}}

select * from {{ref("int_the_look_ecommerce__products__dedup")}}