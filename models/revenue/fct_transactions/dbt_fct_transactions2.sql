  {{ config(materialized='table') }}

  select *
  from {{ ref('chargebee_webtransactions2') }} 
  UNION ALL
  select *, null as is_recurring, null as is_chargeback
  , null advertising_id, null mparticle_id, null email
  from {{ ref('revenuecat_mobile_transactions2') }} 