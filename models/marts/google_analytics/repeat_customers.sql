select * from {{ref('customer_purchases')}}
where purchase_count > 1