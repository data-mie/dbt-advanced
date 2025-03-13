{% snapshot orders_snapshot %}
    {{
        config(
            schema='snapshots',
            unique_key='id',
            strategy='timestamp',
            updated_at='_synced_at'
        )
    }}

    select * from {{ source('ecomm', 'orders') }}
 {% endsnapshot %}