-- Sample rows from staging trade flows (typed columns, NAs as NULL)
select
    year,
    exporter_code,
    importer_code,
    product_code,
    trade_value_usd_thousands,
    quantity_metric_tons
from `global-trade-pipeline.staging.stg_trade_flows`
limit 10
