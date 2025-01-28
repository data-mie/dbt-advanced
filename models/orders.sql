{{
    config(
        materialized='incremental',
        unique_key='order_id',
        schema_change ='sync_all_columns'

    )
}}

-- you can start writing _config and click enter on _config_incremental
-- add  schema_change ='sync_all_columns' to make sure if the columns name change, it does not give an error

with orders as (
    select stg.*,
    map.store_name
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
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
        -- store.store_name instead of orders.store_name if i added the with store()
    datediff('day', lag(ordered_at) over (partition by customer_id order by ordered_at), ordered_at) as days_since_last_order 
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
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    where ordered_at > (select dateadd('day', -3, max(ordered_at)) from {{ this }}) 
{% endif %}


-- Some orders to arrive  much later than they should. As a result, we're now worried about 
--our incremental model missing data. We want to check how many days back we need to inspect 
--what the typical difference is between the two columns:

--We run in snowflake :

    --select
    --datediff('day', created_at, _synced_at) as days_lag,
    --count(*)
    --from raw.ecomm.orders
    --group by 1

 --then we checked 3 days was enough */