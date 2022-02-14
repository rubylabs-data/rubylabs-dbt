{{ config(materialized='table') }}

with invoices as
(
    select * from {{ ref('chargebee_invoices_view') }} where status = 'Paid'
)
,
transactions as
(
    select
        customer_id,
        app_name,
        invoice_number,
        max(CASE WHEN type = 'Refund' THEN true ELSE false END) is_refunded,
        max(CASE WHEN type = 'Refund' THEN date ELSE NULL END) refunded_at,
        max(CASE WHEN payment_method = 'Chargeback' THEN true ELSE false END) is_chargeback
    from {{ ref('chargebee_transactions_view')}}
    where status = 'Success'
    and customer_id is not null
    and invoice_number is not null
    group by customer_id, app_name, invoice_number
)
,
chargebee_join as
(
select
    a.id,
    a.app_name,
    a.customer_id,
    date(a.invoice_date) invoice_date,
    date(a.paid_on) paid_on,
    date(a.start_date) start_date,
    a.status,
    a.country,
    a.email,
    a.invoice_number,
    a.recurring as is_recurring,
    a.amount,
    a.tax_total,
    b.mparticle_id,
    b.advertising_id,
    d.plan_id,
    CASE WHEN d.status = 'In Trial'     THEN true           ELSE false END is_in_trial,
    CASE WHEN d.status = 'Active'       THEN true           ELSE false END is_active,
    CASE WHEN d.status = 'Cancelled'    THEN true           ELSE false END is_cancelled,
    CASE WHEN d.status = 'Non Renewing' THEN true           ELSE false END is_non_renewing,
    CASE WHEN d.Status = 'Cancelled'    THEN d.Cancelled_At ELSE NULL  END cancelled_at,
    c.is_refunded,
    c.refunded_at,
    c.is_chargeback
from invoices a
left join {{ ref('chargebee_customers_view') }} b
    on a.customer_id = b.customer_id
    and a.app_name = b.app_name
left join transactions c
    on a.customer_id = c.customer_id
    and a.app_name = c.app_name
    and a.invoice_number = c.invoice_number
left join {{ ref('chargebee_subscriptions_view')}} d
    on a.customer_id = d.customer_id
    and a.app_name = d.app_name
)

select
    timestamp(MIN(paid_on) OVER(PARTITION BY app_name, customer_id)) min_date, 
    timestamp(paid_on) as date,
    CAST(null AS TIMESTAMP) as end_time,
    customer_id as subscriber_id,
    app_name,
    'web' as platform,
    country,
    plan_id,
    'USD' as currency, 
    amount as price,
    amount - tax_total as proceeds,
    refunded_at,
    is_refunded,
    CAST(null AS TIMESTAMP) as billing_issues_detected_at,
    CAST(null as BOOLEAN) as is_billing_issue,
    MAX(timestamp(cancelled_at)) OVER(PARTITION BY app_name, customer_id, plan_id, id) cancelled_at,
    MAX(CASE WHEN is_recurring = FALSE THEN TRUE ELSE FALSE END) OVER(PARTITION BY app_name, customer_id) is_trial_user,
    MAX(CASE WHEN is_recurring = TRUE THEN TRUE ELSE FALSE END) OVER(PARTITION BY app_name, customer_id) is_subscriber,
    id AS store_transaction_id,
    CAST(null as string) as original_store_transaction_id,
    CAST(null AS BOOL) as is_auto_renewable, 
    CAST(null AS BOOL) as is_trial_period,
    is_recurring, 
    is_chargeback,
    advertising_id, 
    mparticle_id, 
    email
from chargebee_join