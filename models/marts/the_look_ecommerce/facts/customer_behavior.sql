with customers as (
    select
        id as user_id
    from {{ref("users")}}
), 

order_dates as (
    select
        user_id,
        min(date(created_at)) as first_order_date,
        max(date(created_at)) as last_order_date
    from {{ref("orders")}}
    group by 1
),

-- Complete is defined as orders with the status "Complete" or "Shipped"
complete_order_frequency as (
    select
        user_id,
        count(order_id) as purchase_frequency
    from {{ref("orders")}}
    where status in ("Complete", "Shipped")
    group by 1
),

complete_order_amounts as (
    select
        user_id,
        sum(sale_price) as order_value,
    from {{ref("order_items")}}
    where status in ("Complete", "Shipped")
    group by 1
),

customer_lifetime_value as (
    select
        coalesce(a.user_id, b.user_id, c.user_id, d.user_id) as user_id,
        ifnull(order_value,0) as order_value,
        ifnull(purchase_frequency,0) as purchase_frequency,
        first_order_date,
        last_order_date,
        ifnull((date_diff(last_order_date, first_order_date, day)+1)/30/12,0) as customer_lifetime
    from customers a
    left join complete_order_amounts b on a.user_id = b.user_id
    left join complete_order_frequency c on a.user_id = c.user_id
    left join order_dates d on a.user_id = d.user_id
)

select * from customer_lifetime_value