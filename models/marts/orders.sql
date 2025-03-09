{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='append_new_columns'
    )
}}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
{% if is_incremental() %}
    where ordered_at >= (select dateadd('day', -3, max(ordered_at)) from {{ this }})
{% endif %}

),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

store_names as (
    select
        *
    from {{ ref('stores') }}
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
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
        store_name
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
    left join store_names on (store_names.store_id = orders.store_id)
),

final as (
    select
        *,
        datediff('day', lag(ordered_at) over (partition by customer_id order by ordered_at), ordered_at) as days_since_last_order,
        current_timestamp() as inserted_at
    from joined
)

select *
from final
