{% snapshot customers_snapshots %}

{{
    config(
        unique_key = 'id',
        strategy = 'check',
        check_cols = 'all',
        schema = 'snapshots',
    )
}}

select * from {{ source('ecomm', 'customers') }}

{% endsnapshot %}