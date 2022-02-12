{{ config(materialized='ephemeral') }}

select
    a.app_name,
    a.customer_id,
    date(a.invoice_date) invoice_date,
    date(a.paid_on) paid_on,
    min(date(start_date)) over (partition by a.app_name, a.email) as start_date,
    a.status,
    a.country,
    a.email,
    a.invoice_number,
    a.recurring as is_recurring,
    case when a.status = 'Paid' then a.amount else 0 end as amount,
    case when a.status = 'Paid' then a.tax_total else 0 end as tax_total,
    row_number() over (
        partition by a.customer_id order by a.invoice_date
    ) as row_number,
    b.mparticle_id,
    b.advertising_id
from {{ ref('chargebee_invoices_view') }} a
left join
    {{ ref('chargebee_customers_view') }} b
    on a.customer_id = b.customer_id
    and a.app_name = b.app_name
where a.status = 'Paid'