with import_dedup as (
    select *  from {{ref('int_google_analytics__events__dedup')}}
),

customer_purchases as (
    select
        event_timestamp,
        user_pseudo_id,
        customer_event_id
    from import_dedup
    where event_name = 'purchase'
),

count_purchases_per_customer as (
    select
        user_pseudo_id,
        min(event_timestamp) as first_purchase_timestamp,
        max(event_timestamp) as last_purchase_timestamp,
        count(customer_event_id) as purchase_count
    from customer_purchases
    group by 1
)

select * from count_purchases_per_customer