{% snapshot customers_snapshot %}

{{
    config(
        unique_key='id',
        strategy='check',
        check_cols=['ID','FIRST_NAME','LAST_NAME','EMAIL','ADDRESS','PHONE_NUMBER','CREATED_AT'],
        schema='snapshots'
    )
}}

select 
    * 
from {{ source('ecomm', 'customers') }}

{% endsnapshot %}