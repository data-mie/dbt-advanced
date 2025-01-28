select
    -- to see all the columns we just need to add here *;
      country,
      state.value:state::varchar as state,
      zip_code.value:zipcode::varchar as zip_code,
      zip_code.value:city::varchar as city
  from raw.geo.countries
  -- value is a column created by flatten function after the flattening where the date we want is
  left join lateral flatten (input => states) as state 
  -- first flatten separates the STATE->state from STATE->zipcode
  left join lateral flatten (input => state.value:zipcodes) as zip_code
  -- second flatten separates every field inside the STATE-> zipcode like STATE-> zipcode-> zipcode
  -- STATE-> zipcode-> city
 