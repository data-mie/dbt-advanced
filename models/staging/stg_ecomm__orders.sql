with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),

order_status as (
    select *
    from {{ ref('order_status') }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status_normalized
    from renamed
    left join order_status on (
        lower(renamed.order_status) = order_status.order_status
    )
),

final as (
    select *
    from normalize_order_status
)

select *
from final
