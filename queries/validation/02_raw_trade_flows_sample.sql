-- Sample rows from raw trade flows (all columns as STRING)
select t, i, j, k, v, q
from `global-trade-pipeline.raw.baci_trade_flows`
limit 10
