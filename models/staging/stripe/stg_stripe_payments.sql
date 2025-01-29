select
    json_data['order_id']::text as order_id,
json_data['id']::number as payment_id,
json_data['method']::text as payment_type,
json_data['amount']::number as payment_amount,
json_data['created_at']::timestamp as created_at
    from {{ source('json', 'payments') }},
    lateral flatten (input => json_data) l1