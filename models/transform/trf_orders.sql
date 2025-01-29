{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

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
store_names as (
select * from {{ ref('s_stores') }}
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.store_id,
        a.store_name,
        orders.total_amount,
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
    left join store_names as a on a.store_id = orders.store_id
),

final as (
    select *
    from joined
)

select 
ifnull(datediff(day, LAG(ordered_at)  OVER (PARTITION BY customer_id ORDER BY ordered_at),ordered_at),0) as days_since_last_order,
*
from final

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where ordered_at > (select dateadd(day, -3, max(ordered_at)) from {{ this }} ) 
{% endif %}
order by customer_id, ordered_at