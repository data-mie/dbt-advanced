select
    country,
    state.value['state']::varchar as state,
    zipcode.value['zipcode']::varchar as zipcode
from raw.geo.countries,
lateral flatten (input => states) as state,
lateral flatten (input => state.value['zipcodes']) as zipcode