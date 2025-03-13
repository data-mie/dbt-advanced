{% snapshot ecomm_cust_snaps %}
    {{
        config(
            schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols='all',
        )
    }}

    select * from {{ source('ecomm', 'customers') }}
 {% endsnapshot %}