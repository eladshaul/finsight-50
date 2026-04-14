{{ config(
    materialized='table',
    partition_by={
      "field": "measurement_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["stock_ticker"]
) }}

with source as (

    select *
    from {{ source('yfinance_raw', 'fact_history') }}),

renamed as (
    select
        cast(date as date) as measurement_date,
        cast(ticker as string) as stock_ticker,
        cast(open as float64) as open_price,
        cast(high as float64) as high_price,
        cast(low as float64) as low_price,
        cast(close as float64) as close_price,
        cast(volume as int64) as trading_volume,
        _dlt_load_id as load_id

    from source
)

select * from renamed