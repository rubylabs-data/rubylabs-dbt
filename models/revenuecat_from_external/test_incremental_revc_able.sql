{{ config(
    materialized = 'incremental',
    unique_key = 'store_transaction_id'
) }}

--Our statement
with events as (
    select * from {{ ref('revenuecat_able_ext_raw_view') }}


--The first day when data appeared
    {% if not is_incremental() %}
    where file_date='2021-12-08'

    {% endif %}
--Conditions for incremental load
    {% if is_incremental() %}


        {%- call statement('get_last_file_date', fetch_result=True) -%}
            select max(file_date) get_last_snapshot_date from {{ this }}--test_120days.test_incremental_revc_able
        {%- endcall -%}

        {%- set last_file_date = load_result('get_last_file_date')['data'][0][0] -%}

    where file_date = date('{{last_file_date}}')+interval 1 day
    --(select max(file_date) from {{ this }})
    -- and file_date <= date('2021-12-31')
    {% endif %}
)
select * from events 
