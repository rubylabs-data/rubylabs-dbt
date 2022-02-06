{{ config(  materialized='incremental',
            unique_key='id',
            enabled=false
            ) }}

select 
    subscription_id, email

from {{ref('chargebee_invoices_view')}}