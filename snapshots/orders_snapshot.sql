{% snapshot orders_snapshot %}
    {{
        config(
            schema='snapshots',
            unique_key='id',
            strategy='timestamp',
            updated_at='_SYNCED_AT'
        )
    }}

    select * from {{ source('ecomm', 'orders') }}
 {% endsnapshot %}