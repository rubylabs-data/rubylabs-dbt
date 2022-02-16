{{ config(materialized='table') }}

select
    'hint' as app_name,
    lower(device_info_platform) as platform,
    upper(device_info_locale_country) as country,
    upper(left(device_info_locale_language, 2)) as language,
    min(date(event_timestamp)) OVER(PARTITION BY mpid, device_info_platform) as created_at,
    min(date(CASE WHEN events_event_type = 'session_start' THEN event_timestamp ELSE NULL END)) 
        OVER(PARTITION BY mpid, device_info_platform) as session_created_at,
    event_timestamp,
    events_event_type,
    events_data_event_name,
    events_data_activity_type,
    events_data_canonical_name,
    events_data_application_transition_type,
    events_data_custom_attributes_Id,
    events_data_custom_attributes_Source,
    events_data_custom_attributes_MessageText,
    mpid,
    message_id,
    source_request_id,  -- idfv
    coalesce(device_info_ios_idfv, device_info_android_uuid) vendor_id,  -- idfa
    coalesce(device_info_ios_advertising_id, device_info_android_advertising_id) ad_id,
    application_info_install_referrer,
    application_info_package,
    case
        when user_identities_0_identity_type = 'email'
        then user_identities_0_identity
        else null
    end as email,
    user_attributes_chargebeeid chargebee_id,
    user_attributes_advertisingid advertising_id,
    user_identities_0_identity_type,
    user_identities_0_identity,
    user_attributes_revenuecat__adjustid adjust_id,
    mp_deviceid,
    user_attributes_reserved_platform,
    user_attributes_revenuecat_original_app_user_id rc_original_app_user_id,
    user_attributes_revenuecat_app_user_id rc_app_user_id,
    user_attributes_revenuecat__mediaSource,
    user_identities_0_created_this_batch,
    user_identities_1_identity_type,
    user_identities_1_identity,
    device_info_push_token,
    device_info_ios_idfv,
    events_data_custom_attributes_ScreenName,
    events_data_custom_attributes_experimentName,
    events_data_custom_attributes_variationName,
    events_data_custom_attributes_experimentId,
    events_data_custom_attributes_variationId,
    user_attributes_revenuecat__adGroup,
    user_attributes_revenuecat__campaign,
    user_attributes_revenuecat__creative,
    attribution_info_service_provider,
    attribution_info_campaign,
    user_attributes_language
from {{source('staging','mparticle_hint_raw')}}
union all
select
    'able' as app_name,
    lower(device_info_platform) as platform,
    upper(device_info_locale_country) as country,
    upper(left(device_info_locale_language, 2)) as language,
    min(date(event_timestamp)) OVER(PARTITION BY mpid, device_info_platform) as created_at,
    min(date(CASE WHEN events_event_type = 'session_start' THEN event_timestamp ELSE NULL END)) 
        OVER(PARTITION BY mpid, device_info_platform) as session_created_at,
    event_timestamp,
    events_event_type,
    events_data_event_name,
    events_data_activity_type,
    events_data_canonical_name,
    events_data_application_transition_type,
    events_data_custom_attributes_Id,
    events_data_custom_attributes_Source,
    events_data_custom_attributes_MessageText,
    mpid,
    message_id,
    source_request_id,  -- idfv
    coalesce(device_info_ios_idfv, device_info_android_uuid) vendor_id,  -- idfa
    coalesce(device_info_ios_advertising_id, device_info_android_advertising_id) ad_id,
    application_info_install_referrer,
    application_info_package,
    case
        when user_identities_0_identity_type = 'email'
        then user_identities_0_identity
        else null
    end as email,
    user_attributes_chargebeeid chargebee_id,
    user_attributes_advertisingid advertising_id,
    user_identities_0_identity_type,
    user_identities_0_identity,
    user_attributes_revenuecat__adjustid adjust_id,
    cast(null as string) as mp_deviceid,
    user_attributes_reserved_platform,
    user_attributes_revenuecat_original_app_user_id rc_original_app_user_id,
    user_attributes_revenuecat_app_user_id rc_app_user_id,
    user_attributes_revenuecat__mediaSource,
    user_identities_0_created_this_batch,
    user_identities_1_identity_type,
    user_identities_1_identity,
    device_info_push_token,
    device_info_ios_idfv,
    events_data_custom_attributes_ScreenName,
    events_data_custom_attributes_experimentName,
    events_data_custom_attributes_variationName,
    events_data_custom_attributes_experimentId,
    events_data_custom_attributes_variationId,
    user_attributes_revenuecat__adGroup,
    user_attributes_revenuecat__campaign,
    user_attributes_revenuecat__creative,
    cast(null as string) as attribution_info_service_provider,
    cast(null as string) as attribution_info_campaign,
    cast(null as string) as user_attributes_language
from {{source('staging','mparticle_able_raw')}}