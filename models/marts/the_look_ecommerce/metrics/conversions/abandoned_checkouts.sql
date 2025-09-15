-- Abandoned Checkouts = those that added items to their carts but did not proceed to purchase
with import_events as (
    select * from {{ref("fct_events")}}
),

import_event_product as (
    select * from {{ref("int_extract_product_from_event_uri")}}
),

final_event_per_session as (
    select
        date(date_trunc(created_at, month)) as created_month,
        session_id,
        event_type as final_event_type,
        traffic_source,
        user_id,
        city,
        state
    from import_events
    -- this window function is used to get the final event per session
    qualify 1=row_number() over (partition by session_id order by created_at desc, sequence_number desc)
),

join_event_product as (
    select
        created_month,
        session_id,
        final_event_type,
        traffic_source,
        product_id,
        product_name,
        product_category,
        product_brand,
        product_department,
        user_id,
        city,
        state
    from final_event_per_session
    left join import_event_product using(session_id)
),

abandoned_checkouts as (
    select * from join_event_product
    where final_event_type = "cart"
)

select * from abandoned_checkouts