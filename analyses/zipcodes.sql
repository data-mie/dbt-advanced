with zipcodes as (
    select
        *
    from {{ ref('stg_geo__countries') }}
)

select
    *
from zipcodes