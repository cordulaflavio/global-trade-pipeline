with countries as (
    select
        country_code,
        country_name,
        iso2,
        iso3
    from {{ ref('stg_countries') }}
),

regions as (
    select
        country_code,
        region,
        continent
    from {{ ref('country_regions') }}
)

select
    c.country_code,
    c.country_name,
    c.iso2,
    c.iso3,
    r.region,
    r.continent
from countries c
left join regions r on c.country_code = r.country_code
