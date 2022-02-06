{{ config(materialized='ephemeral') }}

select invoice_number, count(distinct customer_id) count
from {{source('staging','chargebee_invoices')}}
group by 1
having count = 1