{{ 
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

select
    1 as id,
    50 as amount_new,
    2024 as event_time

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where event_time > (select max(event_time) from {{ this }}) 
{% endif %}