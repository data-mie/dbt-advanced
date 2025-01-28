{% snapshot snapshot_customers %}
    {{
        config(
            schema='snapshots',
            unique_key='id',
            strategy='check',
            invalidate_hard_deletes=False,
            check_cols = 'all'
        )
    }}

    select * from {{ source('ecomm', 'customers') }}
 {% endsnapshot %}



 