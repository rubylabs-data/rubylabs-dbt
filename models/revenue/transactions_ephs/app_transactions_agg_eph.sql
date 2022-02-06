{{ config(materialized='ephemeral') }}

select
    date,
    end_time,
    subscriber_id,
    app_name,
    processing,
    currency,
    country,
    plan_id,
    is_auto_renewable,
    is_trial_period,
    store_transaction_id,
    original_store_transaction_id,
    max(price) price,
    max(proceeds) proceeds,
    max(case when refunded_at is not null then 1 else 0 end) is_refunded,
    max(
        case
            when date_trunc(date, month) = date_trunc(billing_issues_detected_at, month)
            then 1
            else 0
        end
    ) is_billing_issue,
    max(billing_issues_detected_at) billing_issues_detected_at,
    max(refunded_at) refunded_at,
    max(unsubscribe_detected_at) cancelled_at
from {{ ref('app_transactions_eph') }}
{{ dbt_utils.group_by(n=12) }}

