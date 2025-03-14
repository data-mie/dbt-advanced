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
            when _dbt_source_relation ilike '%orders_us' then 1
            when _dbt_source_relation ilike '%orders_de' then 2
            when _dbt_source_relation ilike '%orders_au' then 3
        end as store_id
    from sources
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from add_store_id
),

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation='renamed',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
)

select
*
from deduplicated