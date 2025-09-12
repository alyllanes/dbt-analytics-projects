{{
    config(
        materialized='table',
        unique_key='id'
    )
}}

select 
    *
from {{ref("int_the_look_ecommerce__events__dedup")}}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where created_at > (select max(created_at) from {{ this }}) 
{% endif %}