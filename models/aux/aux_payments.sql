Select
JSON_DATA:order_id::text as order_id,
JSON_DATA:id::number as payment_id,
JSON_DATA:method::text as payment_type,
JSON_DATA:amount::decimal(10,2) as payment_amount, -- it seems that there are no decimal cases
JSON_DATA:created_at::timestamp as created_at 
from raw.stripe.payments