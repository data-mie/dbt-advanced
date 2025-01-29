select 
json_data['order_id']::varchar as order_id,
json_data['id']::number as payment_id,
json_data['method']::varchar as payment_type,
json_data['amount']::number(12,2) * 100 as payment_amount,
json_data['created_at']::timestamp  as created_at
from {{ source('stripes', 'payments') }}
