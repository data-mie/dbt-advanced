select 
    country,
    state.value['state']::varchar as state,
    zipcodes.value['zipcode']::varchar as zip_code,
    zipcodes.value['city']::varchar as city
from {{ ref('stg_geo__countries') }},
lateral flatten(input => states ) as state,
lateral flatten(input => state.value['zipcodes']) as zipcodes