{% snapshot snapshot_orders %}
    {{
        config(
            schema='snapshot',
            unique_key='id',
            strategy='timestamp',
            updated_at='_synced_at'
        )
    }}

    select * from {{ source('ecomm', 'orders_us') }}
 {% endsnapshot %}