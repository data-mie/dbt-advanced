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

order_status as (
    select *
    from {{ ref('order_status') }}
),

renamed as (
    select
        source.* exclude (store_id),
        id as order_id,
        coalesce(source.store_id,stores.store_id) as store_id,
        created_at as ordered_at
    from source
    left join {{ ref('stores') }} stores
        on stores.country_code = upper(right(_dbt_source_relation,2))
),

normalize_order_status as (
    select
        *,
        -- quick & dirty, will fix later - Mike
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status_final
    from renamed
    left join order_status on 
        lower(renamed.status) = order_status.order_status
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
