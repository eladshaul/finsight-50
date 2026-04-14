{{ config(
    materialized='table',
    cluster_by=['sector']
) }}

with profile_source as (
    select 
        ticker,
        short_name,
        sector,
        industry,
        full_time_employees,
        long_business_summary,
        city,
        country,
        market_cap,
        _dlt_load_id
    from {{ source('yfinance_raw', 'company_info_dim') }}
),

renamed as (
    select
        ticker as stock_ticker,
        cast(short_name as string) as company_name,
        cast(sector as string) as sector,
        cast(industry as string) as industry,
        cast(full_time_employees as int64) as employee_count,
        cast(long_business_summary as string) as business_summary,
        cast(city as string) as city,
        cast(country as string) as country,
        cast(market_cap as float64) as market_cap,
        _dlt_load_id
    from profile_source
)

select *
from renamed