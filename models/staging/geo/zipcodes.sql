select
    country,
    state.value:state::varchar as state,
    zip_code.value:zipcode::varchar as zip_code,
    zip_code.value:city::varchar as city
from raw.geo.countries,
    lateral flatten (input => states) as state,
    lateral flatten (input => state.value:zipcodes) as zip_code