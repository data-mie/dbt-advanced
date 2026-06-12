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
    current_timestamp() as ingested_at
from orders
