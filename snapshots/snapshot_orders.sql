{% snapshot snapshot_orders %}
    {{
        config(
            schema='snapshots',
            unique_key='ID',
            strategy='timestamp',
            updated_at = '_SYNCED_AT'
        )
    }}

    select * from {{ source('ecomm', 'orders') }}
 {% endsnapshot %}