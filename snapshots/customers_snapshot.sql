 {% snapshot customers_snapshot %}
    {{
        config(
            schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols=['first_name','last_name','email','address','phone_number']
        )
    }}

       select * from {{ source('ecomm', 'customers') }}
 {% endsnapshot %}
 
 __
 
