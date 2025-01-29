{% snapshot orders_snapshot %}

{{
    config(
        unique_key='id',
        schema='snapshots',
        strategy='timestamp',
        updated_at='_synced_at'
    )
}}

select * from {{ source('ecomm', 'orders_au') }}

{% endsnapshot %}