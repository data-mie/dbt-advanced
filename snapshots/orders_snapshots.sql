{% snapshot orderes_snapshot %}
    {{
        config(
            unique_key='id',
            strategy='timestamp',
            updated_at = '_SYNCED_AT',
            schema='snapshots',
        )
    }}

    select * from {{source("ecomm",'orders')}}
 {% endsnapshot %}