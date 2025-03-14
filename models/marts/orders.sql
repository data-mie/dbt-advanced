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
        -- this filter will only be applied on an incremental run
        where ordered_at >= (select dateadd('day', -3, max(ordered_at)) from {{ this }}) 
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

store_name as (
    select *
    from {{ref('stores')}}
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
        store_name.store_name,
        greatest_ignore_nulls(orders._synced_at, deliveries_filtered._synced_at) as source_last_updated,
        current_timestamp() as last_updated,
        datediff('day', lag(ordered_at) over (
            partition by customer_id
            order by ordered_at
            ),
            ordered_at
        ) as days_since_last_order

    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
    left join store_name on (orders.store_id = store_name.store_id)
),

final as (
    select 
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as pk_orders,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as hk_customer,
    *
    from joined
)

select *
from final
