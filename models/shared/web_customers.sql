{{ config(materialized='view') }}

select 
    app_name,
    customer_id,
    country,
    created_at,
    auto_collection,
    card_status,
    customer_portal_status
from {{ref('chargebee_customers_view')}}