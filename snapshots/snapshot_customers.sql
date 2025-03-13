{% snapshot snapshot_customers %}
    {{
        config(
            schema='snapshots',
            unique_key='ID',
            strategy='check',
            check_cols='all'
        )
    }}

    select * from {{ source('ecomm', 'customers') }}
 {% endsnapshot %}