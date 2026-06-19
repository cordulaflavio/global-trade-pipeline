with source as (
    select
        t,
        i,
        j,
        k,
        v,
        q
    from {{ source('raw', 'baci_trade_flows') }}
),

renamed as (
    select
        cast(t as int64)                    as year,
        cast(i as int64)                    as exporter_code,
        cast(j as int64)                    as importer_code,
        k                                   as product_code,
        cast(nullif(v, 'NA') as float64)    as trade_value_usd_thousands,
        cast(nullif(q, 'NA') as float64)    as quantity_metric_tons
    from source
)

select
    year,
    exporter_code,
    importer_code,
    product_code,
    trade_value_usd_thousands,
    quantity_metric_tons
from renamed
