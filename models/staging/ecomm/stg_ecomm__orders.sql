with source as (
    select *
    from 
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
        *,
        id as order_id,
        created_at as ordered_at
    from source
),

add_store_id as (
    select
        * exclude (store_id),
        case 
            when _dbt_source_relation like '%orders_us' then 1
            when _dbt_source_relation like '%orders_de' then 2
            when _dbt_source_relation like '%orders_au' then 3
        end as store_id
    from renamed
),

deduplicated_orders as (
    {{  dbt_utils.deduplicate(
            relation='add_store_id',
            partition_by='order_id',
            order_by='_synced_at desc',
    ) }}
),

order_status as (
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        deduplicated_orders.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from deduplicated_orders
    left join order_status on (lower(deduplicated_orders.status) = order_status.order_status)
),

final as (
    select *
    from normalize_order_status
)

select *
from final
