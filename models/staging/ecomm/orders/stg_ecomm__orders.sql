with 
union_all as (
    select id,total_amount,'USD' as currency,status,created_at,customer_id,_synced_at from {{ref("stg_ecomm__orders_us")}}
    union all
    select id,total_amount,currency,status,created_at,customer_id,_synced_at from {{ref("stg_ecomm__orders_de")}}
    union all
    select id,total_amount,currency,status,created_at,customer_id,_synced_at from {{ref("stg_ecomm__orders_au")}}
),
renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from union_all
),
max_id as (
select id, max(_synced_at) _SYNCED_AT
from renamed
group by 1 
),
final as (
select a.* 
from renamed a
inner join max_id  b
on a.id = b.id and a._SYNCED_AT = b._SYNCED_AT
)
select *
from final
