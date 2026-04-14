{{ config(
    materialized='table',
    partition_by={
      "field": "report_date",
      "data_type": "date",
      "granularity": "year"
    },
    cluster_by = "stock_ticker"
) }}

WITH base_data AS (

    SELECT 
        f.*,
        p.sector,
        p.employee_count
    FROM {{ ref('int_annual_financials_unified') }} f
    LEFT JOIN {{ ref('stg_company_profile') }} p
        ON f.stock_ticker = p.stock_ticker
),
company_metrics AS (

    SELECT 
        b.*,
        SAFE_DIVIDE(historical_market_cap, 1e9) AS market_cap_billions,
        
        -- 1. Valuation
        SAFE_DIVIDE(close_price, eps) AS pe_ratio,
        SAFE_DIVIDE(close_price, SAFE_DIVIDE(total_revenue, shares_outstanding)) AS ps_ratio,
        SAFE_DIVIDE(close_price, SAFE_DIVIDE(operating_cash_flow, shares_outstanding)) AS pcf_ratio,
        
        -- 2. Profitability Ratios (higher is generally better)
        SAFE_DIVIDE(gross_profit, total_revenue) * 100 AS gross_margin,
        SAFE_DIVIDE(net_income, total_revenue) * 100 AS net_profit_margin,
        SAFE_DIVIDE(free_cash_flow, total_revenue) * 100 AS free_cash_flow_margin,
        
        -- 3. Per Share
        SAFE_DIVIDE(total_revenue, shares_outstanding) AS revenue_per_share,
        SAFE_DIVIDE(free_cash_flow, shares_outstanding) AS free_cash_flow_per_share,
        
        -- 4. Management Effectiveness
        SAFE_DIVIDE(net_income, stockholders_equity) * 100 AS roe,
        SAFE_DIVIDE(net_income, total_assets) * 100 AS roa,
        
        -- 6. Financial Strength
        SAFE_DIVIDE(current_assets, current_liabilities) AS current_ratio,
        SAFE_DIVIDE(total_debt, stockholders_equity) AS debt_to_equity,
        
        -- 7. Efficiency
        SAFE_DIVIDE(total_revenue, total_assets) AS asset_turnover,

        -- 8. Dividend
        SAFE_DIVIDE(dividends_paid, net_income) * 100 AS dividend_payout_ratio,
        SAFE_DIVIDE(SAFE_DIVIDE(dividends_paid, shares_outstanding), close_price) * 100 AS dividend_yield,

        ROW_NUMBER() OVER (PARTITION BY stock_ticker ORDER BY report_date DESC) as recency_rank

    FROM base_data b
)
SELECT 
    *,
    CASE 
        WHEN recency_rank = 1 THEN 'FY0 (Latest)'
        WHEN recency_rank = 2 THEN 'FY-1'
        WHEN recency_rank = 3 THEN 'FY-2'
        WHEN recency_rank = 4 THEN 'FY-3'
        ELSE CONCAT('FY-', CAST(recency_rank - 1 AS STRING))
    END AS financial_year_label
FROM company_metrics
