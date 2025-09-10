with events as (
  select 
    id,
    session_id,
    sequence_number,
    city,
    state,
    created_at,
    traffic_source,
    event_type,
    date(date_trunc(created_at, month)) as created_month, 
    cast(split(uri, "/")[2] as INT64) as uri_extracted_id
  from {{ref("int_the_look_ecommerce__events")}}
  where event_type = "product"
  qualify 1=row_number() over (partition by session_id order by sequence_number desc)
),

products as (
  select
    id,
    brand,
    category,
    name
  from {{ref("int_the_look_ecommerce__products")}}
),

join_products as (
  select * from events a
  left join products b on a.uri_extracted_id = b.id
),

count_product as (
  select 
    brand,
    category,
    name,
    count(session_id) as session_count
  from join_products
  group by 1,2,3
)

select * from count_product