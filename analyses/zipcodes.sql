with source as (
    select *
    from {{ source('geo', 'countries') }}
)

  select
      country,
      state.value:state::varchar as state,
      zip_code.value:zipcode::varchar as zip_code,
      zip_code.value:city::varchar as city
  from source
  left join lateral flatten (input => states) as state
  left join lateral flatten (input => state.value:zipcodes) as zip_code