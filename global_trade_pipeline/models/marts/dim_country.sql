with countries as (
    select
        country_code,
        country_name,
        iso2,
        iso3
    from {{ ref('stg_countries') }}
)

select
    country_code,
    country_name,
    iso2,
    iso3
from countries
