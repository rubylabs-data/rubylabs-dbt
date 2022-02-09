  {{ config(materialized='table') }}

with last1 as (

 select 
 coalesce(a.app_name, b.app_name) app_name
 , coalesce(a.customer_id, b.customer_id) customer_id
 ,	coalesce(a.invoice_date, null) invoice_date
 ,	coalesce(a.paid_on, null) paid_on
,	coalesce(a.start_date, b.Created_At) start_date
 ,	coalesce(a.status, b.status) status
 ,	coalesce(a.country, b.country) country
 ,	coalesce(a.email, b.email) email
 ,	coalesce(CAST(a.invoice_number AS STRING), 'n. a.') invoice_number
 , coalesce(is_recurring, FALSE) is_recurring
 , coalesce(a.amount, 0) amount
  , coalesce(a.tax_total, 0) tax_total
 , coalesce(a.row_number, 0) row_number
 , coalesce(a.mparticle_id, b.mparticle_id) as mparticle_id
 , coalesce(a.advertising_id, b.advertising_id) as advertising_id
 ,	coalesce(a.gateway, 'n. a.') gateway
 ,	coalesce(payment_method, 'n. a.') payment_method
 ,	coalesce(a.is_refunded, 0) is_refunded
  ,	a.refunded_at refunded_at
 ,	coalesce(a.is_chargeback, 0) is_chargeback
 , b.plan_id
  , CASE WHEN b.status in ('In Trial') THEN 1 ELSE 0 END is_in_trial
  , CASE WHEN b.status in ('Active') THEN 1 ELSE 0 END is_active
 , CASE WHEN b.status in ('Cancelled') THEN 1 ELSE 0 END is_cancelled
 , CASE WHEN b.status in ('Non Renewing') THEN 1 ELSE 0 END is_non_renewing
 , CASE WHEN b.Status = 'Cancelled' THEN b.Cancelled_At ELSE NULL END cancelled_at
 from 
( select * from {{ ref('chargebee_invoices_transactions') }}   where is_refunded in (0,1) ) a
 FULL JOIN 
 --(select distinct * from `astrology-coach-v2.chargebee.chargebee_subscriptions`)  b 
-- select count(1), count(distinct Subscription_Id) cnt_ids from
 (
 select distinct a.*, b.email, b.country, b.created_at created_at_b
 , b.mparticle_id,	b.advertising_id,	b.CID
 from {{ source('staging', 'chargebee_subscriptions_view') }} a
 left join {{ source('staging', 'chargebee_customers_view') }} b
 ON a.customer_id = b.customer_id and a.app_name = b.app_name
-- select * from `data-analytics-265916.dwh_v2.chargebee_customers_view` limit 1000
 )  b 

 -- select * from `astrology-coach-v2.chargebee.chargebee_subscriptions` limit 100;
  ON a.customer_id = b.customer_id and a.app_name = b.app_name
  )
  select * from last1 where is_refunded in (0,1)