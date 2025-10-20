select 
order_id, customer_id, order_status, order_date, required_date, 
coalesce(shipped_date,'0') as shipped_date
, store_id, staff_id
from {{source('local_bike','orders')}}