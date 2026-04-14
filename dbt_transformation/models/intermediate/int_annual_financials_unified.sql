{{ config(
    materialized='table',
    cluster_by=["stock_ticker", "report_date"]
) }}

with annual_financials as (
    select
    inc.stock_ticker,
    inc.report_type, 
    inc.report_date, 
    inc.total_revenue,
    inc.gross_profit,
    inc.operating_expenses,
    inc.operating_income_ebit,
    inc.net_income,
    inc.eps,
    inc.interest_expense,
    inc.rd_expenses,
    inc.sga_expenses,
    bal.total_assets,
    bal.current_assets,
    bal.cash_and_cash,
    bal.inventory,
    bal.net_ppe,
    bal.goodwill,
    bal.current_liabilities,
    bal.total_debt,
    bal.long_term_debt,
    bal.net_debt,
    bal.stockholders_equity,
    bal.retained_earnings,
    bal.shares_outstanding,
    cf.operating_cash_flow,
    cf.investing_cash_flow,
    cf.financing_cash_flow,
    cf.free_cash_flow,
    cf.capex,
    cf.sbc,
    cf.depreciation_amortization,
    abs(cf.dividends_paid) as dividends_paid,
    cf.stock_buybacks,
    cf.debt_issued,
    cf.debt_repaid
 

    from
    {{ ref('stg_income_statement_annual') }} as inc
    left join
    {{ ref('stg_balance_sheet_annual') }} as bal
    on inc.stock_ticker = bal.stock_ticker and extract(YEAR FROM inc.report_date) = extract(YEAR FROM bal.report_date)
    left join
    {{ ref('stg_cash_flow_annual') }} as cf
    on cf.stock_ticker = inc.stock_ticker and extract(YEAR FROM cf.report_date) = extract(YEAR FROM inc.report_date)
),

prices_joined as (
    select a.*,
            HIST.measurement_date,
            hist.close_price
    from annual_financials a
    left join {{ ref('stg_stock_history') }} as hist
    on hist.stock_ticker = a.stock_ticker 
    and  hist.measurement_date >= a.report_date
    AND hist.measurement_date <= DATE_ADD(a.report_date, INTERVAL 7 DAY)
)

select *,
(shares_outstanding * close_price) AS historical_market_cap
from prices_joined
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY stock_ticker, report_date 
    ORDER BY measurement_date ASC
) = 1

