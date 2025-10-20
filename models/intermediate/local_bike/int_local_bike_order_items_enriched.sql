-- -- jointure order_items + products + brands + categories
/*select * 
FROM {{ref('stg_local_bike_order_items')}} oi
LEFT JOIN {{ref('stg_local_bike_products')}} p ON oi.product_id = p.product_id
LEFT JOIN {{ref('stg_local_bike_brands')}} b ON p.brand_id = b.brand_id
LEFT JOIN {{ref('stg_local_bike_categories')}} cat ON p.category_id = cat.category_id
--LEFT JOIN {{ref('int_local_bike_orders_enriched')}} o ON oi.order_id = o.order_id*/

-- models/intermediate/int_local_bike_order_items_enriched.sql

with order_items as (
    select * from {{ ref('stg_local_bike_order_items') }}
),
products as (
    select * from {{ ref('stg_local_bike_products') }}
),
brands as (
    select * from {{ ref('stg_local_bike_brands') }}
),
categories as (
    select * from {{ ref('stg_local_bike_categories') }}
),
orders as (
    select * from {{ ref('int_local_bike_orders_enriched') }}
)

select
    oi.order_id,
    oi.item_id,
    oi.product_id,
    p.product_name,
    b.brand_name,
    cat.category_name,
    oi.quantity,
    oi.list_price,
    oi.discount,
    (oi.quantity * oi.list_price * (1 - oi.discount)) as net_sales,
    orders.store_id,
    orders.store_name,
    orders.city,
    orders.state,
    orders.order_date,
    orders.staff_id,
    orders.customer_id,
    orders.order_status,
    orders.order_delay_days    
from order_items oi
left join products p on oi.product_id = p.product_id
left join brands b on p.brand_id = b.brand_id
left join categories cat on p.category_id = cat.category_id
left join orders on oi.order_id = orders.order_id

