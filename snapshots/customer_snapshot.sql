{% snapshot customers_snapshot %}

{{
    config(
        unique_key='id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'address', 'phone_number'],
        schema='snapshots'
    )
}}

select 
    *
from {{ source('ecomm', 'customers') }}

{% endsnapshot %}