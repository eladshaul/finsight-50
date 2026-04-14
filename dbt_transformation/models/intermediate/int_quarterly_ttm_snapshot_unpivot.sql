{{ config(
    materialized='table',
    cluster_by = ['stock_ticker', 'latest_report_date', 'financial_category']
) }}

WITH snapshot AS (
    SELECT 
        stock_ticker,
        latest_report_date,
        current_price,
        shares_outstanding,
        
        -- Income Statement (TTM)
        ttm_total_revenue,
        ttm_gross_profit,
        ttm_net_income,
        ttm_operating_expenses,
        ttm_operating_income_ebit,
        ttm_eps,
        ttm_interest_expense,
        ttm_rd_expenses,
        ttm_sga_expenses,
        
        -- Balance Sheet
        cash_and_cash,
        current_assets,
        total_assets,
        current_liabilities,
        total_debt,
        stockholders_equity,
        
        -- Cash Flow (TTM)
        ttm_operating_cash_flow,
        ttm_free_cash_flow,
        ttm_dividends_paid,
        
        -- Market Data
        current_market_cap
        
    FROM {{ ref('int_quarterly_ttm_snapshot') }}
)

-- INCOME STATEMENT ITEMS
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement' AS financial_category,
    'Total Revenue' AS financial_item,
    ttm_total_revenue AS amount
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'Gross Profit',
    ttm_gross_profit
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'Operating Income (EBIT)',
    ttm_operating_income_ebit
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'Operating Expenses',
    ttm_operating_expenses
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'R&D Expenses',
    ttm_rd_expenses
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'SG&A Expenses',
    ttm_sga_expenses
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'Net Income',
    ttm_net_income
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'EPS',
    ttm_eps
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Income Statement',
    'Interest Expense',
    ttm_interest_expense
FROM snapshot

-- BALANCE SHEET ITEMS
UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Balance Sheet',
    'Cash & Cash Equivalents',
    cash_and_cash
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Balance Sheet',
    'Current Assets',
    current_assets
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Balance Sheet',
    'Total Assets',
    total_assets
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Balance Sheet',
    'Current Liabilities',
    current_liabilities
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Balance Sheet',
    'Total Debt',
    total_debt
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Balance Sheet',
    'Stockholders Equity',
    stockholders_equity
FROM snapshot

-- CASH FLOW STATEMENT ITEMS
UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Cash Flow Statement',
    'Operating Cash Flow',
    ttm_operating_cash_flow
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Cash Flow Statement',
    'Free Cash Flow',
    ttm_free_cash_flow
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Cash Flow Statement',
    'Dividends Paid',
    ttm_dividends_paid
FROM snapshot

-- MARKET DATA ITEMS
UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Market Data',
    'Current Price',
    current_price
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Market Data',
    'Shares Outstanding',
    shares_outstanding
FROM snapshot

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Market Data',
    'Market Cap',
    current_market_cap
FROM snapshot

