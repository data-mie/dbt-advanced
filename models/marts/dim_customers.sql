select 
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as pk_customers, 
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as hk_customers, 
    *, 
    current_timestamp() as last_updated,
    greatest_ignore_nulls(
        most_recent_order_at, created_at, survey_date::timestamp
    ) as source_last_updated 
    from {{ ref('trf_customers') }}