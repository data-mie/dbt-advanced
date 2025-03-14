with source as
    (select *
  --  from {{ source('ecomm', 'orders_us') }}
--),

from {{ dbt_utils.union_relations(
    relations=[
        source('ecomm', 'orders_us'),
        source('ecomm', 'orders_au'),
        source('ecomm', 'orders_de')
        ],
    )
}}),

add_store_id as (
    select * exclude (store_id),
    case 
        when right(_dbt_source_relation, 2) = 'us' then 1
        when right(_dbt_source_relation, 2) = 'de' then 2
        when right(_dbt_source_relation, 2) = 'au' then 3
    end as store_id
    from source
),

deduplicated as (
   {{ dbt_utils.deduplicate(
    relation='add_store_id',
    partition_by='id',
    order_by='_synced_at desc',
   )
}}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from deduplicated
),

order_status as (
    select
        *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as orderstatus
        from renamed
    left join order_status
        on (lower(renamed.order_status) = order_status.order_status)
),

final as (
    select *
    from normalize_order_status
)

select *
from final