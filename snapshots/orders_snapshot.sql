{% snapshot orders_snapshot %}
--configs
-- unique_id for_multiple collumns['order_number','store_id']
{{
    config(
        unique_key='id', 
        strategy='timestamp',
        updated_at ='_synced_at',
        schema= 'snapshots'
    )
}}
select *
from {{source ('ecomm','orders')}}

{% endsnapshot %}