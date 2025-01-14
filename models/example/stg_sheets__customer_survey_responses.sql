with source as (
  select
    *
  from {{ source('sheets', 'customer_survey_responses') }}
),

transformed as (
  select
    * exclude (survey_date),
    {{ parse_date('survey_date') }} as survey_date
  from source
),

final as (
  select
    *
  from transformed
)

select
  *
from final