with source as (
    select
        country_code,
        country_name,
        country_iso2,
        country_iso3
    from {{ source('raw', 'country_codes') }}
),

renamed as (
    select
        cast(country_code as int64) as country_code,
        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
            country_name,
            'Ã´', 'ô'), 'Ã¼', 'ü'), 'Ã§', 'ç'), 'Ã©', 'é'), 'Ã¨', 'è'),
            'Ã®', 'î'), 'Ã ', 'à'), 'Ã¢', 'â'), 'Ã«', 'ë'), 'Ã±', 'ñ')
        as country_name,
        country_iso2                as iso2,
        country_iso3                as iso3
    from source
)

select
    country_code,
    country_name,
    iso2,
    iso3
from renamed
