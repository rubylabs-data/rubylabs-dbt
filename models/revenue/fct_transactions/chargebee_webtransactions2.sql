  {{ config(materialized='table') }}
WITH core as (
select 
    coalesce( paid_on, date(start_date)) date
    , customer_id subscriber_id
    ,	app_name
    ,	'web' as platform
    , 'USD' currency 
    , country
    , plan_id
    --, 'n. a.' as store_transaction_id
    , CONCAT(app_name, '_', invoice_number, '_', email) AS store_transaction_id
    , 'n. a.' as original_store_transaction_id
    , is_recurring
    , MAX(amount) price
    , MAX(amount) - MAX(tax_total)  proceeds
    , MAX(tax_total) tax_total
    , MAX(is_refunded) is_refunded
    , MAX(refunded_at) refunded_at
--    , MAX(CASE WHEN date_trunc(date, month) = date_trunc(billing_issues_detected_at, month) THEN 1 ELSE 0 END) is_billing_issue
    , MAX(is_chargeback) is_chargeback
    , MAX(is_cancelled) is_cancelled
--    , MAX(NULL) is_billing_issue
    , MAX(cancelled_at) cancelled_at
    , max(advertising_id) advertising_id
    , max(mparticle_id) mparticle_id
    , max(email) email
    from {{ ref('chargebee_webtransactions2a') }}
--    select * from `data-analytics-265916.core_web.web_transaction2a` limit 1000;
-- select * from `data-analytics-265916.core_web.web_transaction2a` where is_refunded = 1 limit 1000;
--    select * from `data-analytics-265916.core_web.web_transaction2a` where paid_on is not null and paid_on != start_date limit 1000;
-- select * from `data-analytics-265916.core_web.web_transaction2a` where customer_id = '258848c3-67d3-4f78-b44d-65e607c70c5c'
-- select * from `data-analytics-265916.dwh.fct_transactions2` where subscriber_id = '258848c3-67d3-4f78-b44d-65e607c70c5c'
--    WHERE is_recurring = TRUE
    group by 1,2,3,4,5,6,7,8,9,10
)
    select 
    timestamp(MIN(date) OVER(PARTITION BY app_name, subscriber_id, platform)) min_date
    , timestamp(date) date
    , CAST(null AS TIMESTAMP) as end_time
    , subscriber_id, app_name, platform
    , CASE WHEN country = '' THEN 'n. a.' ELSE coalesce( country, 'n. a.') END as country
    , plan_id
    , currency, price, proceeds
    
--    , CAST(null AS TIMESTAMP) as refunded_at
    , refunded_at
    , is_refunded
    
    , CAST(null AS TIMESTAMP) as billing_issues_detected_at
    , null as is_billing_issue  -- dunning?!
    
    , MAX(timestamp(cancelled_at)) OVER(PARTITION BY app_name, subscriber_id, platform, plan_id, original_store_transaction_id) cancelled_at
        , MAX(CASE WHEN is_recurring = FALSE THEN 1 ELSE 0 END) OVER(PARTITION BY app_name, subscriber_id, platform) is_trial_user
    , MAX(CASE WHEN is_recurring = TRUE THEN 1 ELSE 0 END) OVER(PARTITION BY app_name, subscriber_id, platform) is_subscriber
    , store_transaction_id, original_store_transaction_id
    , CAST(null AS BOOL) as is_auto_renewable, CAST(null AS BOOL) as is_trial_period
    , is_recurring, is_chargeback
    , advertising_id, mparticle_id, email
    from core