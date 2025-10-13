
select 
 DATETIME(pickup_limit_date, "Europe/Paris") AS picked_up_limited_at_x,
 --quantity as item_quantity,
 price as unit_price_y,
 shipping_cost as shipping_cost_y,
 --quantity as item_quantity,
 (price * quantity) + shipping_cost as total_order_item_amount_y
from {{ source('sales_database', 'order_item') }}
