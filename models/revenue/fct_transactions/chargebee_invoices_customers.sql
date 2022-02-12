  select 
  a.app_name 
  , a.customer_id--, date(a.start_date) start_date
  , date(a.invoice_date) invoice_date, date(a.paid_on) paid_on
  , min(date(start_date)) OVER(PARTITION BY a.app_name, a.email) as start_date
  , a.status, a.country, a.email
  , a.invoice_number
  , a.Recurring as is_recurring
  , CASE WHEN a.status = 'Paid' THEN a.amount ELSE 0 END as amount
  , CASE WHEN a.status = 'Paid' THEN a.Tax_Total ELSE 0 END as tax_total
  , ROW_NUMBER() OVER(PARTITION BY a.customer_id ORDER BY a.invoice_date) as row_number 
  , b.mparticle_id, b.advertising_id
-- select * from `data-analytics-265916.dwh_v2.chargebee_invoices_view` limit 1000;
--select * from `data-analytics-265916.dwh_v2.chargebee_invoices_view` where customer_id = '970f68e7-82de-41bd-926b-dd0a81e58f1a' limit 1000;

from {{ ref('chargebee_invoices_view') }} a
-- select * from `data-analytics-265916.dwh_v2.chargebee_customers_view` limit 1000;
 

 left join  {{ ref('chargebee_customers_view') }} b
  ON a.customer_id = b.customer_id AND a.app_name = b.app_name 
  where a.status = 'Paid'

