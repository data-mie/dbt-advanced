{% snapshot customers_snapshot %}

{{
    config(
        unique_key='id',
        schema='snapshots',
        strategy='check',
        check_cols='all'
    )
}}

select * from {{ source('ecomm', 'customers') }}

{% endsnapshot %}


