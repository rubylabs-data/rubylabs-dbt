{{ config(materialized='view') }}

select
    split(site, '-') [safe_offset(0)] app_name,
    subscriptions_id subscription_id,
    customers_id customer_id,
    subscriptions_plan_id plan_id,
    safe_cast(subscriptions_plan_quantity as int64) plan_quantity,
    subscriptions_status status,
    subscriptions_start_date start_date,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_trial_start) trial_start,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_trial_end) trial_end,
    safe.parse_datetime(
        '%d-%b-%Y %H:%M', subscriptions_current_term_start
    ) curr_term_start,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_current_term_end) curr_term_end,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_next_billing_at) next_billing,
    safe_cast(subscriptions_remaining_billing_cycles as int64) remaining_billing_cycles,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_created_at) created_at,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_started_at) started_at,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_activated_at) activated_at,
    safe.parse_datetime('%d-%b-%Y %H:%M', subscriptions_cancelled_at) cancelled_at,
    split(site, '-') [safe_offset(1)] acc,
    subscriptions_cancel_reason cancel_reason,
    safe_cast(subscriptions_next_billing_amount as float64) next_billing_amount,
    subscriptions_po_number po_number,
    safe_cast(subscriptions_mrr as float64) mrr,
    subscriptions_currency currency,
    safe_cast(subscriptions_plan_unit_price as float64) subscription_plan_unit_price,
    subscriptions_setup_fee subscription_setup_fee,
    subscriptions_pause_date subscription_pause_date,
    subscriptions_resume_date subscriptions_resume_date,
    safe_cast(subscriptions_plan_amount as float64) subscription_plan_amount,
    subscriptions_plan_quantity_in_decimal subscription_plan_quantity_in_decimal,
    subscriptions_plan_unit_price_in_decimal subscription_plan_unit_price_in_decimal,
    subscriptions_plan_amount_in_decimal subscription_plan_amount_in_decimal,
    load_ts,
    update_ts
from {{ source('staging', 'chargebee_subscriptions') }}