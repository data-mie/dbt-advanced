{{ config(materialized='incremental', unique_key='order_id') }}

with orders as (
    select *,
    datediff('day', lag(ordered_at) over (partition by customer_id order by ordered_at desc), ordered_at) days_since_last_order
    from {{ ref('stg_ecomm__orders') }}
    {% if is_incremental() %}
    where ordered_at > (select dateadd('day', -3, max(ordered_at)) from {{ this }})
    {% endif %}
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
stores as (
    select *
    from {{ref('stores')}}
),
joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.store_id,
        stores.store_name,
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
        orders.days_since_last_order
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
    left join stores
    on orders.store_id = stores.store_id
),

final as (
    select *
    from joined
)

select *
from final
order by customer_id ,ordered_at desc
