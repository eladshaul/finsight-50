{{ config(
    materialized='table',
    cluster_by=['stock_ticker']
) }}

with quarterly_source as (
    select 
        ticker,
        report_date,
        total_assets,
        current_assets,
        cash_and_cash_equivalents,
        inventory,
        net_ppe,
        goodwill,
        current_liabilities,
        total_debt,
        long_term_debt,
        net_debt,
        stockholders_equity,
        retained_earnings,
        ordinary_shares_number,
        _dlt_load_id
    from {{ source('yfinance_raw', 'fact_balance_sheet_quarter') }}
),

renamed as (
    select
        ticker as stock_ticker,
        'quarterly' as report_type,
        cast(report_date as date) as report_date,
        
        -- נכסים
        cast(total_assets as float64) as total_assets,
        cast(current_assets as float64) as current_assets,
        cast(cash_and_cash_equivalents as float64) as cash_and_cash,
        cast(inventory as float64) as inventory,
        cast(net_ppe as float64) as net_ppe,
        cast(goodwill as float64) as goodwill,
        
        -- התחייבויות
        cast(current_liabilities as float64) as current_liabilities,
        cast(total_debt as float64) as total_debt,
        cast(long_term_debt as float64) as long_term_debt,
        cast(net_debt as float64) as net_debt,
        
        -- הון
        cast(stockholders_equity as float64) as stockholders_equity,
        cast(retained_earnings as float64) as retained_earnings,
        cast(ordinary_shares_number as float64) as shares_outstanding,
        
        _dlt_load_id

    from quarterly_source
    where total_assets is not null
)


select *
from renamed
