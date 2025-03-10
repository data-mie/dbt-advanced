with source as (
    select *
    from {{ source('stripe', 'payments') }}
),

renamed as (
    select
        json_data['order_id']::text as order_id
    from source

),

final as (
    select *
    from renamed
)

select *
from final