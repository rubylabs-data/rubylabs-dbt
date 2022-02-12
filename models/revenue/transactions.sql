{{ config(materialized='table') }}

select 
  {{ dbt_utils.star(ref('chargebee_webtransactions2')) }}
from {{ ref('chargebee_webtransactions2') }} 
UNION ALL
select
  {{ dbt_utils.star(ref('app_transactions')) }}
from {{ ref('app_transactions') }} 
