{{ config(materialized = 'table')}}

select
    min(date) over (partition by app_name, subscriber_id, processing) min_date,
    date,
    end_time,
    subscriber_id,
    app_name,
    {{ platform_lookup("processing") }} as platform,
    country,
    plan_id,
    currency,
    price,
    proceeds,
    refunded_at,
    is_refunded,
    billing_issues_detected_at,
    is_billing_issue,
    max(cancelled_at) over (
        partition by
            app_name, subscriber_id, processing, plan_id, original_store_transaction_id
    ) cancelled_at,
    store_transaction_id,
    original_store_transaction_id,
    is_auto_renewable,
    is_trial_period
from {{ ref("app_transactions_agg_eph") }}