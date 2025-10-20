/*
KPI principaux :
Revenu total (total_revenue)
Volume vendu (total_items_sold)
Panier moyen (avg_order_value)
Retard moyen (avg_delivery_delay)
*/

with sales as (
    select
        order_id,
        store_name,
        city,
        state,
        category_name,
        brand_name,
        order_date,
        quantity,
        net_sales,
        order_delay_days
    from {{ ref('int_local_bike_order_items_enriched') }}
)

select
    date_trunc(order_date, month) as month,
    store_name,
    city,
    state,
    category_name,
    brand_name,
    sum(quantity) as total_items_sold,
    sum(net_sales) as total_revenue,
    count(distinct order_id) as total_orders,
    round(sum(net_sales) / count(distinct order_id), 2) as avg_order_value,
    avg(order_delay_days) as avg_delivery_delay
from sales
group by 1,2,3,4,5,6
