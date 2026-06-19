-- Validate leading zeros are preserved in product codes
select product_code, product_description
from `global-trade-pipeline.staging.stg_products`
where product_code like '0%'
limit 10
