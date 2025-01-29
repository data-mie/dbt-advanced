with source as (
    
    {{ dbt_utils.union_relations(
    relations=[source('ecomm', 'orders_us'),source('ecomm', 'orders_de'),source('ecomm', 'orders_au')],
    exclude=["_loaded_at"]
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
