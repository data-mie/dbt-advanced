with source as (
    select *
    from {{ source('ecomm', 'orders') }}
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
        coalesce(order_status_normalized, 'Unknown') as order_status_normalized
    from renamed
    left join {{ ref('order_status') }} as status_lookup
        on lower(renamed.order_status) = lower(status_lookup.order_status_raw)
),

final as (
    select *
    from normalize_order_status
)

select *
from final
