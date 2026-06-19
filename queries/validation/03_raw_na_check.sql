-- Count NA values in value and quantity columns
select
    countif(v = 'NA') as value_na,
    countif(q = 'NA') as quantity_na,
    count(*)          as total_rows
from `global-trade-pipeline.raw.baci_trade_flows`
