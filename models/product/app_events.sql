{{ config(materialized='view') }}

select
    'hint' as app_name,
    lower(device_info_platform) as platform,
    upper(device_info_locale_country) as country,
    upper(left(device_info_locale_language, 2)) as language,
    event_timestamp,
    events_event_type,
    events_data_event_name,
    events_data_activity_type,
    events_data_application_transition_type,
    mpid,
    source_request_id,  -- idfv
    coalesce(device_info_ios_idfv, device_info_android_uuid) vendor_id,  -- idfa
    coalesce(device_info_ios_advertising_id, device_info_android_advertising_id) ad_id,
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
    user_attributes_revenuecat__adjustid
from {{source('staging','mparticle_hint_raw')}}
union all
select
    'able' as app_name,
    lower(device_info_platform) as platform,
    upper(device_info_locale_country) as country,
    upper(left(device_info_locale_language, 2)) as language,
    event_timestamp,
    events_event_type,
    events_data_event_name,
    events_data_activity_type,
    events_data_application_transition_type,
    mpid,
    source_request_id,  -- idfv
    coalesce(device_info_ios_idfv, device_info_android_uuid) vendor_id,  -- idfa
    coalesce(device_info_ios_advertising_id, device_info_android_advertising_id) ad_id,
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
    user_attributes_revenuecat__adjustid
from {{source('staging','mparticle_able_raw')}} ;