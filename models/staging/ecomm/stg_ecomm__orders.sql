with 

source as (

    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_ecomm__orders_au'),
                ref('stg_ecomm__orders_de'),
                ref('stg_ecomm__orders_us')
            ]
        )
    }}

),

renamed as (

    select
        *
    from source

),

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation='renamed',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
)


select * from deduplicated
