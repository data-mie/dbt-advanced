with source as (
  -- select *
  --  from {{ source('ecomm', 'orders_us') }}

    {{ dbt_utils.union_relations(
    relations=[source('ecomm', 'orders_us'), source('ecomm', 'orders_au'), source('ecomm', 'orders_de')]
    ) }}
),

renamed as (
    select
        total_amount, customer_id, _synced_at,
        id as order_id,
        created_at as ordered_at,
        status as order_status,
        case when _dbt_source_relation ilike '%orders_us' then '1'
        when _dbt_source_relation ilike '%orders_au' then '2'
        when _dbt_source_relation ilike '%orders_de' then '3' end as store_id,
        case when _dbt_source_relation ilike '%orders_us' then 'USD'
        when _dbt_source_relation ilike '%orders_au' then 'AUD'
        when _dbt_source_relation ilike '%orders_de' then 'EUR' end as currency
    from source
),


final as (
    select *
    from renamed
)

select *
from final
