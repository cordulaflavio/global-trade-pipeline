with products as (
    select
        product_code,
        product_description
    from {{ ref('stg_products') }}
)

select
    product_code,
    product_description,
    left(product_code, 2) as hs2_code
from products
