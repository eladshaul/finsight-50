{{ config(
    materialized='table',
    cluster_by = "stock_ticker"
) }}

WITH balance_sheet_data AS (
    SELECT 
        stock_ticker,
        latest_report_date,
        CASE 
            WHEN financial_item = 'Cash & Cash Equivalents' THEN amount
            ELSE 0 
        END AS cash,
        CASE 
            WHEN financial_item = 'Current Assets' THEN amount
            ELSE 0
        END AS current_assets,
        CASE 
            WHEN financial_item = 'Total Assets' THEN amount
            ELSE 0
        END AS total_assets,
        CASE 
            WHEN financial_item = 'Total Debt' THEN amount
            ELSE 0
        END AS total_debt,
        CASE 
            WHEN financial_item = 'Stockholders Equity' THEN amount
            ELSE 0
        END AS stockholders_equity
    FROM {{ ref('int_quarterly_ttm_snapshot_unpivot') }}
    WHERE financial_category = 'Balance Sheet'
),

aggregated AS (
    SELECT 
        stock_ticker,
        latest_report_date,
        SAFE_DIVIDE(MAX(cash),1e9) AS cash,
        SAFE_DIVIDE(MAX(current_assets) - MAX(cash),1e9) AS current_assets_ex_cash,
        SAFE_DIVIDE(MAX(total_assets) - MAX(current_assets),1e9) AS non_current_assets,
        SAFE_DIVIDE(MAX(total_assets),1e9) AS total_assets,
        SAFE_DIVIDE(MAX(total_debt),1e9) AS total_debt,
        SAFE_DIVIDE(MAX(stockholders_equity),1e9) AS stockholders_equity,
        SAFE_DIVIDE(MAX(total_assets) - (MAX(total_debt) + MAX(stockholders_equity)),1e9) AS other_liabilities,
        SAFE_DIVIDE(MAX(total_assets),1e9) AS total_capitalization
    FROM balance_sheet_data
    GROUP BY stock_ticker, latest_report_date
)
-- PIE CHART 1: LIQUIDITY (Asset Composition)
SELECT 
    stock_ticker,
    latest_report_date,
    'Liquidity Profile' AS chart_type,
    'Cash & Equivalents' AS component,
    cash AS amount,
    total_assets AS total_reference
FROM aggregated

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Liquidity Profile',
    'Current Assets (ex-Cash)',
    current_assets_ex_cash,
    total_assets
FROM aggregated

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Liquidity Profile',
    'Non-Current Assets',
    non_current_assets,
    total_assets
FROM aggregated

-- PIE CHART 2: CAPITAL STRUCTURE (Financing Sources)
UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Capital Structure' AS chart_type,
    'Total Debt' AS component,
    total_debt AS amount,
    total_capitalization AS total_reference
FROM aggregated

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Capital Structure',
    'Stockholders Equity',
    stockholders_equity,
    total_capitalization
FROM aggregated

UNION ALL
SELECT 
    stock_ticker,
    latest_report_date,
    'Capital Structure',
    'Other Liabilities',
    other_liabilities,
    total_capitalization
FROM aggregated
