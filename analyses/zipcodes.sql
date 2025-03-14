with zip_code as (
    select 
        *
    from {{ ref('stg_geo__countries') }}
)
select 
    * 
from zip_code