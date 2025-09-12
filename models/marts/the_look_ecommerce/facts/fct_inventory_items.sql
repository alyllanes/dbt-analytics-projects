{{
    config(
        materialized='incremental',
        unique_key='id',
        schema='the_look_ecommerce'
    )
}}

select 
    * 
from {{ref("int_the_look_ecommerce__inventory_items__dedup")}}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where created_at > (select max(created_at) from {{ this }}) 
{% endif %}