    select
    country, 
    states.value['state']::text as state_name,
    zip_code.value['zipcode']::text as zip_code,
    zip_code.value['city']::text as city,
    zip_code.value['timezone']::text as timezone
    from raw.geo.countries
    left join lateral flatten (input => states) as states
    left join lateral flatten (input => states.value['zipcodes']) as zip_code



