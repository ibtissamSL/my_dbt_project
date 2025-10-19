select 
concat(store_id,'-',product_id) as stock_id,
store_id, product_id, quantity
from {{source('local_bike','stocks')}}