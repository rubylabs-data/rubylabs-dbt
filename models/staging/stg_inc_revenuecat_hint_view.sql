

{{
    /* config block initiating the the type of model (incremental) and the unique_key to avoind duplicates*/
    config(
        materialized='incremental',
        unique_key='rc_original_app_user_id'
    )
}}


/*the specific model in question to be that incrementation will be run on*/
select * from {{ source('staging', 'revenuecat_hint') }}

{% if is_incremental() %}

  /*this filter will only be applied on an incremental run*/
  where insert_timestamp > (select max(insert_timestamp) from {{ this }})

{% endif %}


