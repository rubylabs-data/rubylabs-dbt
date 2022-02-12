{{ config(materialized='ephemeral') }}

select * from {{ref('hint_transactions_eph')}}
union all
select * from {{ref('able_transactions_eph')}}