{{ config(materialized='table') }}

select
    customer_id,
    count(*) as order_count,
    sum(amount) as total_amount
from {{ source('raw', 'orders') }}
group by customer_id
