{{ config(materialized='view') }}

select
    split(site, '-') [safe_offset(0)] app_name,
    transaction_id,
    customer_id,
    safe_cast(invoice_number as int64) invoice_number,
    safe.parse_timestamp('%d-%b-%Y %H:%M', date) date,
    type,
    payment_method,
    safe_cast(amount as float64) amount,
    status,
    error_text,
    gateway,
    card_type,
    customer_email,
    currency,
    safe_cast(amount_unused as float64) amount_unused,
    safe_cast(amount_capturable as float64) amount_capturable,
    gateway_account_id,
    split(site, '-') [safe_offset(1)] acc
from {{ source('staging', 'chargebee_transactions') }}