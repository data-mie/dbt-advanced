
with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}  stg
    
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

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        map.store_name,
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
    left join {{ ref('stores') }} map  on orders.store_id=map.store_id
),

final as (
    select *
    from joined
)
select *,   datediff('day', lag(ordered_at) over (partition by customer_id order by ordered_at), ordered_at) as days_since_last_order
from final



--It's useful for analysis to know how many days had passed between an order and the prior order (for a given customer).

--Using a window function, add a column days_since_last_order to the orders model.