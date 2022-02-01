    {{ config(materialized = 'ephemeral')}}
    
    select 
    date, end_time, subscriber_id,	app_name,	processing, currency, country, plan_id
    , is_auto_renewable, is_trial_period 
    , store_transaction_id, original_store_transaction_id
    , MAX(price) price, MAX(proceeds) proceeds
    , MAX(CASE WHEN refunded_at is not null then 1 else 0 END) is_refunded
    , MAX(CASE WHEN date_trunc(date, month) = date_trunc(billing_issues_detected_at, month) THEN 1 ELSE 0 END) is_billing_issue
    , MAX(billing_issues_detected_at) billing_issues_detected_at
    , MAX(refunded_at) refunded_at
    , MAX(unsubscribe_detected_at) cancelled_at
    from {{ref('app_transactions_eph')}} 
    group by 1,2,3,4,5,6,7,8,9,10,11,12