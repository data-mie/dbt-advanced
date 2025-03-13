with states as (

SELECT 
    country
    ,state.value:state::text as state
    ,state.value as aux
FROM 
    raw.geo.countries
    ,LATERAL FLATTEN(input => states) AS state

)

select states.country
      ,states.state
      ,zipcode.value:zipcode::text as zipcode
      ,zipcode.value:city::text as city
from
    states
    ,LATERAL FLATTEN(input => aux['zipcodes']) AS zipcode