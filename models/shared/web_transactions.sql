{{ config(materialized='view') }}

select 
    app_name,
    transaction_id,
    customer_id,
    invoice_number,
    date,
    type,
    payment_method,
    amount,
    status,
    error_text,
    gateway,
    card_type,
    currency,
    amount_unused,
    amount_capturable
from {{ref('chargebee_transactions_view')}}