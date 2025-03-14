{{
    config(
        materialized='incremental',
        unique_key='pk_orders',
        on_schema_change='append_new_columns'
    )
}}

with orders as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as pk_orders,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as hk_customer,
        ordered_at,
        store_id,
        order_status,
        total_amount,
        _synced_at
    from {{ ref('stg_ecomm__orders') }}
    {% if is_incremental() %}
        where ordered_at >= (select dateadd('day', -3, max(ordered_at)) from {{ this }})
    {% endif %}
),

deliveries as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as pk_orders,
        delivered_at,
        picked_up_at,
        _synced_at,
        delivery_status
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select *
    from deliveries
    where delivery_status = 'delivered'
),

stores as (
    select *
    from {{ ref('stores') }}
),

joined as (
    select
        orders.pk_orders,
        orders.hk_customer,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        orders.store_id,
        stores.store_name,
        greatest_ignore_nulls(
            orders._synced_at, deliveries_filtered._synced_at
        ) as source_last_updated,
        current_timestamp() as last_updated,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
          datediff(
            'day',
            lag(ordered_at) over (
            partition by hk_customer
            order by ordered_at
            ),
            ordered_at
        ) as days_since_last_order
    from orders
    left join deliveries_filtered
        on orders.pk_orders = deliveries_filtered.pk_orders
    left join stores
        on orders.store_id = stores.store_id
),

final as (
    select *
    from joined
)

select *
from final
