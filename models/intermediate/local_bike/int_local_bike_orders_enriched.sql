-- jointure orders + customers + staffs + stores
/*select * 
FROM {{ref("stg_local_bike_order")}} o
LEFT JOIN {{ref('stg_local_bike_customers')}} c ON o.customer_id = c.customer_id
LEFT JOIN {{ref('stg_local_bike_staffs')}} s ON o.staff_id = s.staff_id
LEFT JOIN {{ref('stg_local_bike_stores')}} st ON o.store_id = st.store_id*/

-- models/intermediate/int_local_bike_orders_enriched.sql

with orders as (
    select order_id,customer_id,staff_id,store_id,order_status,order_date,
    required_date, shipped_date
    from {{ ref('stg_local_bike_order') }}
),
customers as (
    select customer_id,first_name,last_name from {{ ref('stg_local_bike_customers') }}
),
staffs as (
    select staff_id,first_name,last_name from {{ ref('stg_local_bike_staffs') }}
),
stores as (
    select store_id,store_name,city,state from {{ ref('stg_local_bike_stores') }}
)

select
    o.order_id,
    o.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    o.staff_id,
    s.first_name || ' ' || s.last_name as staff_name,
    o.store_id,
    st.store_name,
    st.city,
    st.state,
    o.order_status,
    o.order_date,
    o.required_date,
    o.shipped_date,
    --DATE_DIFF(cast(o.shipped_date as date), o.order_date, DAY) as order_delay_days
    DATE_DIFF(CAST(NULLIF(shipped_date, 'NULL') AS DATE),order_date,DAY)AS order_delay_days
from orders o
left join customers c on o.customer_id = c.customer_id
left join staffs s on o.staff_id = s.staff_id
left join stores st on o.store_id = st.store_id

