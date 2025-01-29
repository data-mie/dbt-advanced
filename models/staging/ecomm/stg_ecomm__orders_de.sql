with 

source as (

    select *,
        2 as store_id
    from {{ source('ecomm', 'orders_de') }}

),

store_names as (
    select * 
    from {{ ref('stores') }}
),

renamed as (

    select
        source.*,
        store_names.store_name,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
    left join store_names
        on store_names.store_id = source.store_id

),

final as (
    select *
    from renamed
)

select *
from final
