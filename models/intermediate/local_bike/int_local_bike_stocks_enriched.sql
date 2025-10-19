select * 
FROM {{ref('stg_local_bike_stocks')}} st
LEFT JOIN {{ref('stg_local_bike_order')}}  s ON st.store_id = s.store_id
LEFT JOIN {{ref('stg_local_bike_products')}} p ON st.product_id = p.product_id
LEFT JOIN {{ref('stg_local_bike_brands')}}  b ON p.brand_id = b.brand_id
LEFT JOIN {{ref('stg_local_bike_categories')}} c ON p.category_id = c.category_id
