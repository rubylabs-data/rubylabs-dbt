{{ config(materialized='table') }}

select 
  {{ dbt_utils.star(ref('web_transactions')) }}
from {{ ref('web_transactions') }} 
UNION ALL
select
  {{ dbt_utils.star(ref('app_transactions')) }}
from {{ ref('app_transactions') }} 
