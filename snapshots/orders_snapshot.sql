{% snapshot orders_snapshot %}
    {{
        config(
            target_schema='snapshots',
            unique_key='id',
            strategy='timestamp',
            updated_at='_synced_at'
        )
    }}

    select * from {{ source('ecomm', 'orders') }}
 {% endsnapshot %}