{{ config(materialized='view') }}

select
    *
from {{ source('staging', 'revenuecat_able') }}