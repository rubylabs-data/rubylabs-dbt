{{ config(materialized='ephemeral')}}

select distinct civ.invoice_number, customer_id
from {{source('staging','chargebee_invoices')}} civ
join {{ref('chargebee_single_user_invoices')}} csui on civ.invoice_number = csui.invoice_number
