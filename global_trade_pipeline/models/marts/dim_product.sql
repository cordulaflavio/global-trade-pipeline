with products as (
    select
        product_code,
        product_description,
        left(product_code, 2) as hs2_code
    from {{ ref('stg_products') }}
),

hs2 as (
    select
        hs2_code,
        hs2_description
    from {{ ref('hs2_descriptions') }}
),

joined as (
    select
        p.product_code,
        p.product_description,
        p.hs2_code,
        h.hs2_description,
        safe_cast(p.hs2_code as int64) as hs2_num
    from products p
    left join hs2 h on p.hs2_code = h.hs2_code
)

-- Group the ~97 HS chapters into ~11 broad sectors (goods only; BACI has no services).
-- Boundaries approximate the Atlas of Economic Complexity sector scheme via HS2 ranges.
select
    product_code,
    product_description,
    hs2_code,
    hs2_description,
    case
        when hs2_num between  1 and 24 then 'Agriculture & Food'
        when hs2_num between 25 and 27 then 'Minerals & Fuels'
        when hs2_num between 28 and 40 then 'Chemicals & Plastics'
        when hs2_num between 41 and 43 then 'Textiles, Apparel & Leather'
        when hs2_num between 44 and 49 then 'Wood & Paper'
        when hs2_num between 50 and 67 then 'Textiles, Apparel & Leather'
        when hs2_num between 68 and 71 then 'Stone, Glass & Gems'
        when hs2_num between 72 and 83 then 'Metals'
        when hs2_num = 84 then 'Machinery'
        when hs2_num = 85 then 'Electronics'
        when hs2_num between 86 and 89 then 'Transport & Vehicles'
        else 'Instruments & Other'
    end as product_category
from joined
