with orders as (
    select stg.*, map.store_name
    from {{ ref('stg_ecomm__orders') }} as stg
    left join {{ref('stores')}} as map 
    on stg.store_id = map.store_id
),

-- or i could just add this table
--store as (
--    select * from {{ref('stores')}}
--),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select *
    from deliveries
    where delivery_status = 'delivered'
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        orders.store_name,
        -- store.store_name instead of orders.store_name if i added the with store()
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
    --  left join store on orders.store_id = store.store_id if I  used the with store() view
),

final as (
    select *
    from joined
)

select *
from final
