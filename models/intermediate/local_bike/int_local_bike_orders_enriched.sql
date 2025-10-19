-- jointure orders + customers + staffs + stores
select * 
FROM {{ref("stg_local_bike_order")}} o
LEFT JOIN {{ref('stg_local_bike_customers')}} c ON o.customer_id = c.customer_id
LEFT JOIN {{ref('stg_local_bike_staffs')}} s ON o.staff_id = s.staff_id
LEFT JOIN {{ref('stg_local_bike_stores')}} st ON o.store_id = st.store_id
