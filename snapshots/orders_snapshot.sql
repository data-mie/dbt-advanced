{% snapshot orders_snapshot %}

{{
      config(
      unique_key='id',
      strategy='timestamp',
      updated_at='_synced_at',
      schema='snapshots'
      )
}}

select
  *
from {{ source('ecomm', 'orders') }}

{% endsnapshot %}