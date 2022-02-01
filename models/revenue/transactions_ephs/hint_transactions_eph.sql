{{ config(materialized='ephemeral') }}

select
  'able' as app_name
  , start_time date
--  , start_time
  , end_time
  , store as processing
  , rc_original_app_user_id as subscriber_id
--  , rc_last_seen_app_user_id_alias as subscriber_id2
  , country
  , product_identifier plan_id
  
  , is_auto_renewable
  , is_trial_period
--  , is_in_intro_offer_period ?!
  
  , 'USD' currency
  , price_in_usd as price --gross
  , price_in_usd * takehome_percentage as proceeds

  , refunded_at
  , unsubscribe_detected_at 
  , billing_issues_detected_at
  
  , store_transaction_id
  , original_store_transaction_id
from {{ source('staging', 'revenuecat_hint') }}
where is_sandbox = False and store != 'promotional'