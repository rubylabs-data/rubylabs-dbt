{{ config(materialized='view') }}

select
    {{ dbt_utils.star(source('staging', 'revenuecat_able'), except=['reserved_subscriber_attributes']) }},
    json_extract_scalar(
        replace(reserved_subscriber_attributes, '$', ''), '$.idfa.value'
    ) idfa,
    timestamp_millis(
        safe_cast(
            json_extract_scalar(
                replace(reserved_subscriber_attributes, '$', ''), '$.idfa.updated_at_ms'
            ) as int
        )
    ) idfa_updated_ts,
    json_extract_scalar(
        replace(reserved_subscriber_attributes, '$', ''), '$.idfv.value'
    ) idfv,
    timestamp_millis(
        safe_cast(
            json_extract_scalar(
                replace(reserved_subscriber_attributes, '$', ''), '$.idfv.updated_at_ms'
            ) as int
        )
    ) idfv_updated_ts,
    json_extract_scalar(
        replace(reserved_subscriber_attributes, '$', ''), '$.adjustId.value'
    ) adjustid,
    timestamp_millis(
        safe_cast(
            json_extract_scalar(
                replace(reserved_subscriber_attributes, '$', ''),
                '$.adjustId.updated_at_ms'
            ) as int
        )
    ) adjustid_updated_ts,
    json_extract_scalar(
        replace(reserved_subscriber_attributes, '$', ''), '$.mediaSource.value'
    ) mediasource,
    timestamp_millis(
        safe_cast(
            json_extract_scalar(
                replace(reserved_subscriber_attributes, '$', ''),
                '$.mediaSource.updated_at_ms'
            ) as int
        )
    ) mediasource_updated_ts,
    json_extract_scalar(
        replace(reserved_subscriber_attributes, '$', ''), '$.mparticleId.value'
    ) mparticleid,
    timestamp_millis(
        safe_cast(
            json_extract_scalar(
                replace(reserved_subscriber_attributes, '$', ''),
                '$.mparticleId.updated_at_ms'
            ) as int
        )
    ) mparticleid_updated_ts
from {{ source('staging', 'revenuecat_able') }}