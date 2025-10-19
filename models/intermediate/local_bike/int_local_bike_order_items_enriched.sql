-- -- jointure order_items + products + brands + categories
select * 
FROM {{ref('stg_local_bike_order_items')}} oi
LEFT JOIN {{ref('stg_local_bike_products')}} p ON oi.product_id = p.product_id
LEFT JOIN {{ref('stg_local_bike_brands')}} b ON p.brand_id = b.brand_id
LEFT JOIN {{ref('stg_local_bike_categories')}} cat ON p.category_id = cat.category_id
--LEFT JOIN {{ref('int_local_bike_orders_enriched')}} o ON oi.order_id = o.order_id
