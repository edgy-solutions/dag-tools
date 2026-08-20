{{ config(materialized='table') }}

-- One hop off the seed, so the run produces real dbt lineage
-- (seed -> model) for DataHub to ingest.
select
    customer_id,
    region,
    count(*)    as order_count,
    sum(amount) as total_amount,
    max(loaded_at) as last_order_at
from {{ ref('demo_orders') }}
group by customer_id, region
