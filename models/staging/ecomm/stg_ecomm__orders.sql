with sources as (
    {{ 
        dbt_utils.union_relations(
            relations = [
                source('ecomm', 'orders_us'),
                source('ecomm', 'orders_de'),
                source('ecomm', 'orders_au')
            ]
        )
    }}
),

populate_store_ids as (
    select
        * exclude (store_id),
        case
            when _dbt_source_relation = 'raw.ecomm.orders_us' then 1
            when _dbt_source_relation = 'raw.ecomm.orders_de' then 2
            when _dbt_source_relation = 'raw.ecomm.orders_au' then 3
        end as store_id
    from sources
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from populate_store_ids
),

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation = 'renamed',
            partition_by = 'order_id',
            order_by = "_synced_at desc",
        )
    }}
),

order_status as (
    select
        *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        deduplicated.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status_normalized
    from deduplicated
    left join order_status 
        on lower(deduplicated.status) = order_status.order_status 
),

final as (
    select *
    from normalize_order_status
)

select *
from final
