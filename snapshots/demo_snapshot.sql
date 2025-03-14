{% snapshot demo_snapshot %}
 
{{
    config(
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        schema='snapshots'
    )
}}
 
select * from demo.snapshots.orders
 
{% endsnapshot %}