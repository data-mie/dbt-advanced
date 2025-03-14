{% snapshot orders_snapshots %}

{{
    config(
        unique_key = 'id',
        strategy = 'timestamp',
        updated_at = '_synced_at',
        schema = 'snapshots',
    )
}}

select * from {{ source('ecomm', 'orders_us') }}

{% endsnapshot %}