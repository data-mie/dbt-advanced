with structured as (
select json_data:order_id::varchar(36) as order_id,
json_data:id::number as payment_id,
json_data:method::varchar(50) as payment_type,
json_data:amount::number(15,2) as payment_amount,
json_data:created_at::timestamp as created_at
from {{ source('stripe', 'payments') }}
)
select *
from structured