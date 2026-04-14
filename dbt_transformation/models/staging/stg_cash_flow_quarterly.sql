{{ config(
    materialized='table',
    cluster_by=['stock_ticker']
) }}

with quarterly_source as (
    select 
        ticker,
        report_date,
        operating_cash_flow,
        investing_cash_flow,
        financing_cash_flow,
        free_cash_flow,
        capital_expenditure,
        stock_based_compensation,
        depreciation_and_amortization,
        cash_dividends_paid,
        repurchase_of_capital_stock,
        issuance_of_debt,
        repayment_of_debt,
        _dlt_load_id
    from {{ source('yfinance_raw', 'fact_cash_flow_quarter') }}
),

renamed as (
    select
        ticker as stock_ticker,
        'quarterly' as report_type,
        cast(report_date as date) as report_date,
        
        -- תזרימים מרכזיים
        cast(operating_cash_flow as float64) as operating_cash_flow,
        cast(investing_cash_flow as float64) as investing_cash_flow,
        cast(financing_cash_flow as float64) as financing_cash_flow,
        cast(free_cash_flow as float64) as free_cash_flow,
        
        -- השקעות והוצאות לא מזומניות
        cast(capital_expenditure as float64) as capex,
        cast(stock_based_compensation as float64) as sbc,
        cast(depreciation_and_amortization as float64) as depreciation_amortization,
        
        -- החזר למשקיעים וניהול חוב
        cast(cash_dividends_paid as float64) as dividends_paid,
        cast(repurchase_of_capital_stock as float64) as stock_buybacks,
        cast(issuance_of_debt as float64) as debt_issued,
        cast(repayment_of_debt as float64) as debt_repaid,
        _dlt_load_id

    from quarterly_source
    where operating_cash_flow is not null
    
)


select *
from renamed
