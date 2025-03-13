with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select *
    from deliveries
    where delivery_status = 'delivered'
),

filtered_orders_after_stores as (
    select * 
    from orders
    join stores
    on orders.store_id = stores.store_id
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
        store_name
    from filtered_orders_after_stores as orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
),

final as (
    select *
    from joined
)

select *
from final
