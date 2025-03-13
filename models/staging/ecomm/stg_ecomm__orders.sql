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
        CASE 
        WHEN ORDER_STATUS_NORMALIZED IS NULL THEN 'Unknown'
        ELSE ORDER_STATUS_NORMALIZED
        END AS ORDER_STATUS_NORMALIZED

    from renamed

    left join order_status
    on lower(renamed.order_status) = order_status.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final

