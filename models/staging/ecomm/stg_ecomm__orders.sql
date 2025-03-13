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

order_status as (
    select * from {{ref('order_status')}}
),

normalize_order_status as (
    select
        renamed.*,
        -- quick & dirty, will fix later - Mike
         ifnull(order_status.order_status_normalized , 'Unknown') as  order_status_normalized
        
    from renamed
     left join order_status on order_status.order_status = renamed.status 
),

final as (
    select *
    from normalize_order_status
)

select *
from final
