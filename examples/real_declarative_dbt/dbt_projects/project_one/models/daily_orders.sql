{{ config(materialized='table') }}

select
    date_trunc('day', loaded_at) as order_day,
    count(*)                     as order_count,
    sum(amount)                  as total_amount
from {{ source('raw', 'orders') }}
group by 1
