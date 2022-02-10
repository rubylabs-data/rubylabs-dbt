{{ config(materialized='view') }}

with sub_cus_id as
(
    select distinct subscriptions_id, customers_id as customer_id
    from {{source('staging','chargebee_subscriptions')}}
)

select
    {{dbt_utils.surrogate_key(['site','invoice_number'])}} id,
    split(site, '-') [safe_offset(0)] app_name,
    coalesce(nullif(i.customer_id,''), s.customer_id) customer_id,
    subscription_id subscription_id,
    safe.parse_datetime('%d-%b-%Y %H:%M', invoice_date) invoice_date,
    safe.parse_datetime('%d-%b-%Y %H:%M', start_date) start_date,
    safe.parse_datetime('%d-%b-%Y %H:%M', paid_on) paid_on,
    safe_cast(amount as float64) amount,
    status,
    customer_billing_country country,
    safe.parse_datetime('%d-%b-%Y %H:%M', next_retry) next_retry,
    safe_cast(refunded_amount as float64) refunded_amount,
    safe_cast(recurring as bool) recurring,
    safe_cast(first_invoice as bool) first_invoice,
    --customer_first_name customer_first_name,
    --customer_last_name customer_last_name,
    {{email_hash('customer_email')}} email,
    --customer_company customer_company,
    safe_cast(tax_total as float64) tax_total,
    --vat_number vat_number,
    --po_number po_number,
    safe_cast(amount_due as float64) amount_due,
    safe_cast(adjustments as float64) adjustments,
    safe_cast(credits_applied as float64) credits_applied,
    safe_cast(payments as float64) payments,
    safe_cast(write_off_amount as float64) write_off_amount,
    currency currency,
    safe_cast(net_term_days as int64) net_term_days,
    safe.parse_datetime('%d-%b-%Y %H:%M', due_date) due_date,
    voided_at voided_at,
    safe_cast(invoice_number as int64) invoice_number,
    split(site, '-') [safe_offset(1)] acc,
    dunning_status dunning_status,
    load_ts,
    update_ts
from {{ source('staging', 'chargebee_invoices') }} i
left join sub_cus_id s on i.subscription_id = s.subscriptions_id