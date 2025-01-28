{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select 
1 as id,
50 as amount,
2024 as event_time
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where event_time > (select max(event_time) from {{ this }}) 
{% endif %}