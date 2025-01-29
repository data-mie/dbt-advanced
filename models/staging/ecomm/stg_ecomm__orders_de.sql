with 

source as (

    select * from {{ source('ecomm', 'orders_de') }}

),

renamed as (

    select
        id,
        total_amount,
        currency,
        status,
        created_at,
        customer_id,
        _synced_at

    from source

)

select * from renamed
