with base as (
    select
        oi.order_id,
        oi.order_date,
        oi.store_name,
        oi.city,
        oi.state,
        oi.staff_id,
        oi.customer_id,
        oi.product_id,
        oi.product_name,
        oi.brand_name,
        oi.category_name,
        oi.quantity,
        oi.list_price,
        oi.discount,
        (oi.quantity * oi.list_price) as gross_sales,
        (oi.quantity * oi.list_price * (1 - oi.discount)) as net_sales
    from {{ ref('int_local_bike_order_items_enriched') }} oi
)

select
    date_trunc(order_date, month) as month,
    store_name,
    state,
    brand_name,
    category_name,
    sum(quantity) as total_items_sold,
    sum(gross_sales) as total_gross_sales,
    sum(net_sales) as total_net_sales,
    count(distinct order_id) as total_orders,
    sum(net_sales) / count(distinct order_id) as avg_basket_value
from base
group by 1,2,3,4,5
