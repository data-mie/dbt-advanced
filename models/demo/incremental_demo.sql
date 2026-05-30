{{
    config(
        materialized='table',
    )
}}

with orders as (

    select *
    from {{ ref("stg_demo") }}
)

select

    order_id,
    customer_id,
    order_date,
    amount,
    status,
    ingested_at,
    current_timestamp() updated_at
from orders
