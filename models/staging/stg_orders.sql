
with orders as (
    select * 
    from {{ ref('demo_snapshot') }}
),
stores as (
    select *
    from {{ ref('stores') }}
)

select *
from orders
left join using (store_id)


