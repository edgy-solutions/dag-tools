{{ config(materialized='table') }}

select
    campaign_id,
    sum(spend) as total_spend
from {{ source('raw', 'campaigns') }}
group by campaign_id
