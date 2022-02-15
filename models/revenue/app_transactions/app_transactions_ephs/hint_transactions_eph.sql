{{ config(materialized="ephemeral") }}

select
    'hint' as app_name,
    -- , start_time
    start_time date,
    end_time,
    store as processing,
    -- , rc_last_seen_app_user_id_alias as subscriber_id2
    rc_original_app_user_id as subscriber_id,
    country,
    product_identifier plan_id,
    is_auto_renewable,
    -- , is_in_intro_offer_period ?!
    is_trial_period,
    'USD' currency,  -- gross
    price_in_usd as price,
    price_in_usd * takehome_percentage as proceeds,
    refunded_at,
    unsubscribe_detected_at,
    billing_issues_detected_at,
    store_transaction_id,
    original_store_transaction_id,
    mparticleid,
    adjustid
from  {{ref('revenuecat_hint_view')}}
where is_sandbox = false 
  and store != 'promotional'