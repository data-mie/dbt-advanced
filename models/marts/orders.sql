{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        on_schema_change = 'append_new_columns'
    )
}}

with orders as (
    select 
        orders.*
    from {{ ref('stg_ecomm__orders') }} orders
    {% if is_incremental() %}
        where orders.ordered_at >= (select dateadd('day', -3, max(orders.ordered_at)) from {{ this }})
    {% endif %}
),

stores as (
    select
        *
    from {{ ref('stores') }}
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
        stores.store_name,
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
    left join stores
        on orders.store_id = stores.store_id
),

final as (
    select 
        joined.*,
        current_timestamp() as last_updated
    from joined
)

select *
from final
