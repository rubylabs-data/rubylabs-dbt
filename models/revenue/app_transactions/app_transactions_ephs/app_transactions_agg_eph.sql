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
    mparticleid,
    adjustid,
    max(price) price,
    max(proceeds) proceeds,
    max(case when refunded_at is not null then true else false end) is_refunded,
    max(case when is_trial_period is true then true else false end) is_trial_user,
    max(
        case when is_auto_renewable = true and is_trial_period = false then true else false end
    ) is_subscriber,
    max(
        case
            when date_trunc(date, month) = date_trunc(billing_issues_detected_at, month)
            then true
            else false
        end
    ) is_billing_issue,
    max(billing_issues_detected_at) billing_issues_detected_at,
    max(refunded_at) refunded_at,
    max(unsubscribe_detected_at) cancelled_at
from {{ ref('app_transactions_eph') }}
{{ dbt_utils.group_by(n=14) }}

