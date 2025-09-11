{{
    config(
        materialized='ephemeral'
    )
}}

with import_events as (
    select
        id as event_id,
        session_id,
        cast(split(uri, "/")[2] as int64) as product_id
    from {{ref("fct_events")}}
    where event_type = "product"
),

import_products as (
    select
        id as product_id,
        name as product_name,
        category as product_category,
        brand as product_brand,
        department as product_department
    from {{ref("dim_products")}}
),

final as (
    select
        event_id,
        session_id,
        product_id,
        product_name,
        product_category,
        product_brand,
        product_department
    from import_events
    left join import_products using(product_id)
)

select * from final