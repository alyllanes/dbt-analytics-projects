{{
    config(
        materialized='ephemeral'
    )
}}

with import_source as (
    select * from {{ref("stg_the_look_ecommerce__inventory_items")}}
),

qualified as (
    select * from import_source
    qualify 1=row_number() over (partition by id order by created_at desc)
)

select * from qualified