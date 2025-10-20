/*
Revenu / mois / magasin
Nombre de commandes
Délai moyen
*/

select
    store_name,
    city,
    state,
    date_trunc(order_date, month) as month,
    sum(net_sales) as total_revenue,
    count(distinct order_id) as total_orders,
    avg(order_delay_days) as avg_delay
from {{ ref('int_local_bike_order_items_enriched') }}
group by 1,2,3,4
