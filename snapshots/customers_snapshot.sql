{% snapshot snapshot_name %}
    {{
        config(
            schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols= 'all'
        )
    }}

    select * from {{ source('ecomm', 'customers') }}
 {% endsnapshot %}