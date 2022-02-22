{{ config(
    materialized = 'incremental',
    unique_key = 'store_transaction_id'
) }}

--Our statement
with events as (
    select distinct store_transaction_id
,last_value(rc_original_app_user_id) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) rc_original_app_user_id
,last_value(rc_last_seen_app_user_id_alias) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) rc_last_seen_app_user_id_alias
,last_value(country) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) country
,last_value(product_identifier) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) product_identifier
,last_value(start_time) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) start_time
,last_value(end_time) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) end_time
,last_value(store) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) store
,last_value(is_auto_renewable) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) is_auto_renewable
,last_value(is_trial_period) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) is_trial_period
,last_value(is_in_intro_offer_period) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) is_in_intro_offer_period
,last_value(is_sandbox) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) is_sandbox
,last_value(price_in_usd) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) price_in_usd
,last_value(takehome_percentage) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) takehome_percentage
,last_value(original_store_transaction_id) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) original_store_transaction_id
,last_value(refunded_at) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) refunded_at
,last_value(unsubscribe_detected_at) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) unsubscribe_detected_at
,last_value(billing_issues_detected_at) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) billing_issues_detected_at
,last_value(purchased_currency) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) purchased_currency
,last_value(price_in_purchased_currency) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) price_in_purchased_currency
,last_value(entitlement_identifiers) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) entitlement_identifiers
,last_value(renewal_number) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) renewal_number
,last_value(is_trial_conversion) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) is_trial_conversion
,last_value(presented_offering) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) presented_offering
,last_value(custom_subscriber_attributes) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) custom_subscriber_attributes
,last_value(platform) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) platform
,last_value(idfa) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) idfa
,last_value(idfa_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) idfa_updated_ts
,last_value(idfv) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) idfv
,last_value(idfv_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) idfv_updated_ts
,last_value(adGroup) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) adGroup
,last_value(adGroup_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) adGroup_updated_ts
,last_value(adjustid) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) adjustid
,last_value(adjustid_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) adjustid_updated_ts
,last_value(campaign) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) campaign
,last_value(campaign_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) campaign_updated_ts
,last_value(creative) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) creative
,last_value(creative_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) creative_updated_ts
,last_value(mediasource) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) mediasource
,last_value(mediasource_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) mediasource_updated_ts
,last_value(mparticleid) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) mparticleid
,last_value(mparticleid_updated_ts) over (partition by store_transaction_id order by file_date rows between unbounded preceding and unbounded following) mparticleid_updated_ts
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
