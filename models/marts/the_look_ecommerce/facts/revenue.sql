with orders as (
    select 
        order_id,
        id as order_item_id,
        product_id,
        sale_price,
        created_at as ordered_at
    from {{ref("order_items")}}
    where status in("Complete", "Shipped")
),

inventory as (
    select
        product_id,
        sold_at,
        cost,
        product_retail_price
    from {{ref("inventory_items")}}
    where sold_at is not null
),

revenue as (
    select
        order_id,
        order_item_id,
        coalesce(a.product_id, b.product_id) as product_id,
        1 as quantity,
        cost,
        sale_price,
        sale_price - cost as revenue
    from orders a
    left join inventory b on a.product_id = b.product_id and a.ordered_at = b.sold_at
)

select * from revenue