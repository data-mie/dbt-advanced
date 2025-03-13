with source as (
    select *
    from {{ source('geo', 'countries') }}
),

final as (
    select *
    from source
)

select *
from final