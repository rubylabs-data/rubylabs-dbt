{{ config(materialized='view') }}

select 
    app_name,
    customer_id,
    subscription_id,
    invoice_date,
    start_date,
    paid_on,
    amount,
    status,
    country,
    next_retry,
    refunded_amount,
    recurring,
    first_invoice,
    amount_due,
    adjustments,
    credits_applied,
    payments,
    write_off_amount,
    currency,
    net_term_days,
    due_date,
    voided_at,
    invoice_number,
    dunning_status
from {{ref('chargebee_invoices_view')}}