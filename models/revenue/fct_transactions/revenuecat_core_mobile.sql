  {{ config(materialized='view') }}

  select 
    app_name
  , start_time date
  , end_time
  , store as processing
  , rc_original_app_user_id as subscriber_id
  , country
  , product_identifier plan_id
  , is_auto_renewable
  , is_trial_period 
  , 'USD' currency
  , price_in_usd as price 
  , price_in_usd * takehome_percentage as proceeds
  , refunded_at
  , unsubscribe_detected_at 
  , billing_issues_detected_at
  , store_transaction_id
  , original_store_transaction_id

from 
(
select 'hint' as app_name, *
from {{ source('staging', 'revenuecat_hint') }} 
where is_sandbox = False and store != 'promotional'

UNION ALL
select 'able' as app_name, * from {{ source('staging', 'revenuecat_able') }}
  where is_sandbox = False and store != 'promotional' 
)