/*select * 
FROM {{ref('stg_local_bike_stocks')}} st
LEFT JOIN {{ref('stg_local_bike_order')}}  s ON st.store_id = s.store_id
LEFT JOIN {{ref('stg_local_bike_products')}} p ON st.product_id = p.product_id
LEFT JOIN {{ref('stg_local_bike_brands')}}  b ON p.brand_id = b.brand_id
LEFT JOIN {{ref('stg_local_bike_categories')}} c ON p.category_id = c.category_id*/

-- models/intermediate/int_local_bike_stocks_enriched.sql

select
    st.store_id,
    s.store_name,
    s.city,
    s.state,
    st.product_id,
    p.product_name,
    b.brand_name,
    c.category_name,
    st.quantity
from {{ ref('stg_local_bike_stocks') }} st
left join {{ ref('stg_local_bike_stores') }} s on st.store_id = s.store_id
left join {{ ref('stg_local_bike_products') }} p on st.product_id = p.product_id
left join {{ ref('stg_local_bike_brands') }} b on p.brand_id = b.brand_id
left join {{ ref('stg_local_bike_categories') }} c on p.category_id = c.category_id

