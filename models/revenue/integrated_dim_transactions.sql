{{
    config(
        materialized='incremental'
    )
}}

select subscriber_id
  , transaction_id
  , first_user_purchase_transaction_id
  , country 
  , platform
  , plan_id
  , subscription_starts_at
  , subscription_expires_at
  , transaction_source
  , currency
  , price
  , proceeds_percentage
  , round(price * proceeds_percentage / 100, 2) as proceeds
  , transaction_rank
  , case when refunded_at is null then 0 else 1 end as is_refunded
  , case when unsubscribe_detected_at is null then 0 else 1 end as is_unsubscribed
  , case when billing_issues_detected_at is null then 0 else 1 end is_billing_issue_detected
  , is_auto_renewable
  , is_trial
  , refunded_at
  , unsubscribe_detected_at
  , billing_issues_detected_at
  , inserted_at
from {{ ref('cleansed_transactions') }}

{% if is_incremental() %}

  -- limit to rows inserted since last incremental run
  where inserted_at > (select max(inserted_at) from {{ this }})

{% endif %}
