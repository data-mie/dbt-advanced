{% snapshot orders_snapshot %}

{{
    config(
        unique_key='id',
        strategy='timestamp',
        updated_at='_synced_at',
        schema='snapshots'
    )
}}

    {{
        dbt_utils.union_relations(
            relations=[
                source('ecomm', 'orders_us'),
                source('ecomm', 'orders_de'),
                source('ecomm', 'orders_au')
            ],
        )
    }}

{% endsnapshot %}