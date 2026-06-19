-- Row counts for all raw tables
select 'baci_trade_flows' as table_name, count(*) as row_count
from `global-trade-pipeline.raw.baci_trade_flows`
union all
select 'country_codes', count(*)
from `global-trade-pipeline.raw.country_codes`
union all
select 'product_codes', count(*)
from `global-trade-pipeline.raw.product_codes`
