select a.*,
b.gateway, b.payment_method, b.is_refunded , b.is_chargeback, b.refunded_at
from {{ ref('chargebee_invoices_customers') }} a
left join 
  (
--select app_name, customer_id, invoice_number, max(f_gateway) gateway, max(f_payment_method) payment_method, max(is_refunded) is_refunded 
select app_name, customer_email, invoice_number, max(f_gateway) gateway, max(f_payment_method) payment_method, max(is_refunded) is_refunded, max(is_chargeback) is_chargeback, max(refunded_at) refunded_at from 
(
   select *, row_number() over (partition by app_name, customer_id, invoice_number order by date desc) rn
        , first_value(payment_method) over (partition by app_name, customer_id, invoice_number order by date ) f_payment_method
        , first_value(gateway) over (partition by app_name, customer_id, invoice_number order by date ) f_gateway
        , CASE WHEN type = 'Refund' and status = 'Success' THEN 1 ELSE 0 END is_refunded
        , CASE WHEN type = 'Refund' and status = 'Success' THEN date ELSE NULL END refunded_at
        , CASE WHEN payment_method = 'Chargeback' and status = 'Success' THEN 1 ELSE 0 END is_chargeback
        FROM  {{ ref('chargebee_transactions_view') }} 
--SELECT * from `data-analytics-265916.dwh_v2.chargebee_transactions_view` LIMIT 1000
        where status = 'Success' 
       -- and customer_email = 'crystalruiz351@gmail.com'
--       and customer_id = '94dc3383-8010-4c6d-a4e9-47764cdf1de9'
        )
        group by 1,2,3
    ) b
ON a.invoice_number = b.invoice_number and a.email = b.customer_email and a.app_name = b.app_name