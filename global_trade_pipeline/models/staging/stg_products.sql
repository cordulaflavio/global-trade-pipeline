with source as (
    select
        code,
        description
    from {{ source('raw', 'product_codes') }}
),

renamed as (
    select
        code        as product_code,
        description as product_description
    from source
)

select
    product_code,
    product_description
from renamed
