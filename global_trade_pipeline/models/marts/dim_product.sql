with products as (
    select
        product_code,
        product_description
    from {{ ref('stg_products') }}
),

hs2 as (
    select
        hs2_code,
        hs2_description
    from {{ ref('hs2_descriptions') }}
)

select
    p.product_code,
    p.product_description,
    left(p.product_code, 2)  as hs2_code,
    h.hs2_description
from products p
left join hs2 h on left(p.product_code, 2) = h.hs2_code
