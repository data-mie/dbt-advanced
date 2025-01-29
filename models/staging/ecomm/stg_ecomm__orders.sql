with source as (
  -- select *
  --  from {{ source('ecomm', 'orders_us') }}

    {{ dbt_utils.union_relations(
    relations=[source('ecomm', 'orders_us'), source('ecomm', 'orders_au'), source('ecomm', 'orders_de')]
    ) }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

final as (
    select *
    from renamed
)

select *
from final
