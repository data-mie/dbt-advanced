with source as (
    {{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            source('ecomm', 'orders_au')
        ],
    )
    }}
),

renamed as (
    select
        * exclude (store_id),
        id as order_id,
        created_at as ordered_at,
        status as order_status,
        right(_dbt_source_relation, '2') as country_code,
    from source
),

get_store_id as (
    select renamed.*, stores.store_id as store_id 
    
    from renamed
    
    left join stores
    on renamed.country_code = stores.country_code
),

deduplicate as (
    {{ dbt_utils.deduplicate(
        relation='get_store_id',
        partition_by='order_id',
        order_by='_synced_at desc',
        )
    }}
),

normalize_order_status as (
    select
        deduplicate.*,
        CASE 
        WHEN ORDER_STATUS_NORMALIZED IS NULL THEN 'Unknown'
        ELSE ORDER_STATUS_NORMALIZED
        END AS ORDER_STATUS_NORMALIZED

    from deduplicate

    left join order_status
    on lower(deduplicate.order_status) = order_status.order_status
),

add_metadata as (
    select 
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as pk_orders,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as hk_customer,
    *,
    current_timestamp as last_updated,
    _synced_at as source_last_updated
    from
    normalize_order_status
),

final as (
    select *
    from add_metadata
)

select *
from final


