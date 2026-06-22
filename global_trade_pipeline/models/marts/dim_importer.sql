select
    country_code,
    country_name,
    iso2,
    iso3
from {{ ref('dim_country') }}
