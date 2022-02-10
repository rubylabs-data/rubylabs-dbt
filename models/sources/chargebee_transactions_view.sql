{{ config(materialized='view') }}

with invoice_ids_cus_ids as
(
    select distinct id, customer_id
    from {{ref('chargebee_invoices_view')}}
)

select
    {{dbt_utils.surrogate_key(['site','transaction_id'])}} id,    
    split(site, '-') [safe_offset(0)] app_name,
    transaction_id,
    coalesce(nullif(t.customer_id,''), i.customer_id) customer_id,
    safe_cast(t.invoice_number as int64) invoice_number,
    {{dbt_utils.surrogate_key(['site','invoice_number'])}} invoice_id,
    safe.parse_timestamp('%d-%b-%Y %H:%M', date) date,
    type,
    payment_method,
    safe_cast(amount as float64) amount,
    status,
    error_text,
    gateway,
    card_type,
    --customer_email,
    currency,
    safe_cast(amount_unused as float64) amount_unused,
    safe_cast(amount_capturable as float64) amount_capturable,
    gateway_account_id,
    split(site, '-') [safe_offset(1)] acc,
    load_ts,
    update_ts
from {{ source('staging', 'chargebee_transactions') }} t
left join invoice_ids_cus_ids i 
on i.id = {{dbt_utils.surrogate_key(['site','invoice_number'])}}