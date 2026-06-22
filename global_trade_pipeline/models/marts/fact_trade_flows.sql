{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'year',
      'data_type': 'int64',
      'range': {'start': 2014, 'end': 2025, 'interval': 1}
    },
    cluster_by = ['exporter_code', 'importer_code'],
    on_schema_change = 'sync_all_columns'
  )
}}

with trade_flows as (
    select
        year,
        exporter_code,
        importer_code,
        product_code,
        trade_value_usd_thousands * 1000 as trade_value_usd,
        quantity_metric_tons
    from {{ ref('stg_trade_flows') }}
    {% if is_incremental() %}
        where year >= (select coalesce(max(year), 2014) from {{ this }})
    {% endif %}
)

select
    year,
    exporter_code,
    importer_code,
    product_code,
    trade_value_usd,
    quantity_metric_tons
from trade_flows
