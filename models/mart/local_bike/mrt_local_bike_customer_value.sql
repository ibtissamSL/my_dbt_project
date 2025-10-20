select
oi.customer_id,
o.customer_name,
count(distinct oi.order_id) as total_orders,
sum(oi.net_sales) as totl_spent,
round(avg(oi.net_sales),2) as avg_order_value,
avg(oi.order_delay_days) as avg_delay
from {{ref('int_local_bike_order_items_enriched')}} as oi
left join {{ref('int_local_bike_orders_enriched')}} as o
on oi.customer_id = o.customer_id
group by 1,2