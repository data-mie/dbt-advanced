{% snapshot order_snapshot %}
    {{
        config(
            schema='snapshots',
            unique_key='id',
            strategy='timestamp',
            updated_at='_synced_at'
        )
    }}

    select * from {{ source('ecomm', 'orders') }}
 {% endsnapshot %}


 --- instead of "dbt snapshot --select order_snapshot", we can run "dbt snapshot -s order_snapshot"

-- target_schema =/ schema ---- use always schema