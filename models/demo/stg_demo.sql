select
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    current_timestamp() as ingested_at
from {{ source("demo", "raw_orders") }}
