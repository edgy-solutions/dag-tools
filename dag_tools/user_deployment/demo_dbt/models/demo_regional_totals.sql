{{ config(materialized='table') }}

-- A second hop, so the ingested lineage graph is more than one edge.
select
    region,
    count(*)          as customer_count,
    sum(order_count)  as order_count,
    sum(total_amount) as total_amount
from {{ ref('demo_customer_orders') }}
group by region
