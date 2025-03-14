with sources as (
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

add_store_id as (
    select
        * exclude (store_id),    
        case
            when _dbt_source_relation like '%_us' then 1
            when _dbt_source_relation like '%_de' then 2
            when _dbt_source_relation like '%_au' then 3
        end as store_id            
    from sources
),


renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
    from add_store_id
),

order_status as (
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from renamed
    left join order_status on (
        lower(renamed.status) = lower(order_status.order_status)
    )
),

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation='normalize_order_status',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
),

final as (
    select *
    from deduplicated
)

select *
from final
