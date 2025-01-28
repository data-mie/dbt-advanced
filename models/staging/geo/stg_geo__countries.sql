with 

source as (

    select * from {{ source('geo', 'countries') }}

),

renamed as (

    select
        country,
        states

    from source

)

select * from renamed
