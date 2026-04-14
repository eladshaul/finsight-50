{{ config(
    materialized='table',
    cluster_by=['stock_ticker']
) }}

with quarterly_source as (
    select 
        ticker,
        report_date,
        total_revenue,
        gross_profit,
        operating_expense,
        ebit,
        net_income,
        diluted_eps,
        interest_expense,
        research_and_development,
        selling_general_and_administration,
        _dlt_load_id
    from {{ source('yfinance_raw', 'fact_income_statement_quarter') }} -- וודא שהשם נכון
),

renamed as (
    select
        ticker as stock_ticker,
        'quarterly' as report_type, -- הוספת סוג הדוח
        cast(report_date as date) as report_date,
        cast(total_revenue as float64) as total_revenue,
        cast(gross_profit as float64) as gross_profit,
        cast(operating_expense as float64) as operating_expenses,
        cast(ebit as float64) as operating_income_ebit,
        cast(net_income as float64) as net_income,
        cast(diluted_eps as float64) as eps,
        cast(interest_expense as float64) as interest_expense,
        cast(research_and_development as float64) as rd_expenses,
        cast(selling_general_and_administration as float64) as sga_expenses,
        _dlt_load_id

    from quarterly_source
    where total_revenue is not null
)

select *
from renamed