{{ config(
    materialized = 'incremental',
    unique_key = 'store_transaction_id'
) }}


{% set fields =  ['rc_original_app_user_id', 'rc_last_seen_app_user_id_alias', 'country', 'product_identifier', 
'start_time', 'end_time', 'store', 'is_auto_renewable', 'is_trial_period', 'is_in_intro_offer_period', 
'is_sandbox', 'price_in_usd', 'takehome_percentage', 'original_store_transaction_id', 'refunded_at', 
'unsubscribe_detected_at', 'billing_issues_detected_at', 'purchased_currency', 'price_in_purchased_currency', 
'entitlement_identifiers', 'renewal_number', 'is_trial_conversion', 'presented_offering', 'custom_subscriber_attributes', 
'platform', 'idfa', 'idfa_updated_ts', 'idfv', 'idfv_updated_ts', 'adGroup', 'adGroup_updated_ts', 'adjustid', 
'adjustid_updated_ts', 'campaign', 'campaign_updated_ts', 'creative', 'creative_updated_ts', 'mediasource', 
'mediasource_updated_ts', 'mparticleid', 'mparticleid_updated_ts'] %}

--Our statement
with events as (
    select distinct store_transaction_id
{%- for i in fields -%}
        ,last_value({{i}}) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) {{i}}
{% endfor %}
,max(file_date)  over (partition by store_transaction_id) file_date 
from {{ ref('revenuecat_able_ext_raw_view') }}


--The first day when data appeared
    -- {% if not is_incremental() %}
    -- where file_date='2021-12-08'

    -- {% endif %}
--Conditions for incremental load
    {% if is_incremental() %}


        {%- call statement('get_last_file_date', fetch_result=True) -%}
            select max(file_date) get_last_snapshot_date from {{ this }}--test_120days.test_incremental_revc_able
        {%- endcall -%}

        {%- set last_file_date = load_result('get_last_file_date')['data'][0][0] -%}

    where file_date > date('{{last_file_date}}')
    --(select max(file_date) from {{ this }})
    -- and file_date <= date('2021-12-31')
    {% endif %}
)
select * from events 
