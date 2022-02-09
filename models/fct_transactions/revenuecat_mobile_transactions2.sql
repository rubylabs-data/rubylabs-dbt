  {{ config(materialized='table') }}
with agg as 
(
    select 
    date, end_time, subscriber_id,	app_name,	processing, currency, country, plan_id
    , is_auto_renewable, is_trial_period 
    , store_transaction_id, original_store_transaction_id
    , MAX(price) price, MAX(proceeds) proceeds
    , MAX(CASE WHEN refunded_at is not null then 1 else 0 END) is_refunded
    , MAX(CASE WHEN is_trial_period is true then 1 else 0 END) is_trial_user
    , MAX(CASE WHEN is_auto_renewable = True AND is_trial_period = False then 1 else 0 END) is_subscriber
    , MAX(CASE WHEN date_trunc(date, month) = date_trunc(billing_issues_detected_at, month) THEN 1 ELSE 0 END) is_billing_issue
    , MAX(billing_issues_detected_at) billing_issues_detected_at
    , MAX(refunded_at) refunded_at
    , MAX(unsubscribe_detected_at) cancelled_at
    from {{ ref('revenuecat_core_mobile') }} group by 1,2,3,4,5,6,7,8,9,10,11,12
    )
    select  MIN(date) OVER(PARTITION BY app_name, subscriber_id, processing) min_date
    , date
    , end_time
    , subscriber_id, app_name
      , CASE WHEN processing = "app_store" THEN 'ios'
         WHEN processing = "play_store" THEN 'android'
         WHEN processing = "web" THEN 'web'
         ELSE 'n. a.' END as platform
    
    , CASE WHEN country = '' THEN 'n. a.' ELSE coalesce( country, 'n. a.') END as country
    , plan_id
    , currency, price, proceeds
    , refunded_at
    , is_refunded
    , billing_issues_detected_at
    , is_billing_issue
    , MAX(cancelled_at) OVER(PARTITION BY app_name, subscriber_id, processing, plan_id, original_store_transaction_id) cancelled_at
    , MAX(is_trial_user) OVER(PARTITION BY app_name, subscriber_id, processing) is_trial_user
    , MAX(is_subscriber) OVER(PARTITION BY app_name, subscriber_id, processing) is_subscriber
    , store_transaction_id, original_store_transaction_id
    , is_auto_renewable, is_trial_period
    from agg