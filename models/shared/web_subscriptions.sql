{{ config(materialized='view') }}

select 
    app_name,
    subscription_id,
    customer_id,
    plan_id,
    plan_quantity,
    status,
    start_date,
    trial_start,
    trial_end,
    curr_term_start,
    curr_term_end,
    next_billing,
    remaining_billing_cycles,
    created_at,
    started_at,
    activated_at,
    cancelled_at,
    cancel_reason
    next_billing_amount,
    mrr,
    currency,
    subscription_plan_unit_price,
    subscription_setup_fee,
    subscription_pause_date,
    subscriptions_resume_date,
    subscription_plan_amount,
    subscription_plan_quantity_in_decimal,
    subscription_plan_unit_price_in_decimal,
    subscription_plan_amount_in_decimal
from {{ref('chargebee_subscriptions_view')}}