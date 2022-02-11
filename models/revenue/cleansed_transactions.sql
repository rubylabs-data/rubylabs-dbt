{{
    config(
        materialized='table'
    )
}}

select rc_original_app_user_id as subscriber_id
  , cast(store_transaction_id as text) as transaction_id
  , cast(original_store_transaction_id as text) as first_user_purchase_transaction_id
  , cast(country as text) as country 
  , cast(platform as text) as platform
  , cast(product_identifier as text) as plan_id
  , datetime(start_time) as subscription_starts_at
  , datetime(end_time) as subscription_expires_at
  , cast(store as text) as transaction_source
  , 'USD' as currency
  , round(cast(price_in_usd as real), 2) as price
  , cast(takehome_percentage as real) * 100 as proceeds_percentage
  , cast(renewal_number as integer) as transaction_rank
  , cast(is_auto_renewable as integer) as is_auto_renewable
  , cast(is_trial_period as integer) as is_trial
  , datetime(refunded_at) as refunded_at
  , datetime(unsubscribe_detected_at) as unscribe_detected_at
  , datetime(billing_issues_detected_at) as billing_issues_detected_at
  , datetime('2022-01-01 00:00:00.000') as inserted_at -- dummy timestamp for subsequent incremental model
from {{ source('staging', 'revenuecat_hint') }}
  where is_sandbox = false 
  and transaction_source != 'promotional'
