{{ config(
    materialized='table',
    cluster_by = "stock_ticker"
) }}

WITH base_data AS (
    SELECT 
        f.*,
        p.sector,
        ROW_NUMBER() OVER (PARTITION BY f.stock_ticker ORDER BY report_date DESC) as recency_rank,

        SAFE_DIVIDE(
            total_revenue - LAG(total_revenue) OVER (PARTITION BY f.stock_ticker ORDER BY report_date),
            ABS(LAG(total_revenue) OVER (PARTITION BY f.stock_ticker ORDER BY report_date))
        ) AS yoy_revenue_growth,
        
        SAFE_DIVIDE(
            net_income - LAG(net_income) OVER (PARTITION BY f.stock_ticker ORDER BY report_date),
            ABS(LAG(net_income) OVER (PARTITION BY f.stock_ticker ORDER BY report_date))
        ) AS yoy_net_income_growth,

        SAFE_DIVIDE(
            dividends_paid - LAG(dividends_paid) OVER (PARTITION BY f.stock_ticker ORDER BY report_date),
            ABS(LAG(dividends_paid) OVER (PARTITION BY f.stock_ticker ORDER BY report_date))
        ) AS yoy_dividend_growth

    FROM {{ ref('int_annual_financials_unified') }} f
    LEFT JOIN {{ ref('stg_company_profile') }} p 
        ON f.stock_ticker = p.stock_ticker
),
source AS (
    SELECT stock_ticker,
    CASE 
        WHEN recency_rank = 1 THEN 'FY0 (Latest)'
        WHEN recency_rank = 2 THEN 'FY-1'
        WHEN recency_rank = 3 THEN 'FY-2'
        WHEN recency_rank = 4 THEN 'FY-3'
        ELSE CONCAT('FY-', CAST(recency_rank - 1 AS STRING))
    END AS financial_year_label,
    recency_rank,
    yoy_revenue_growth,
    yoy_net_income_growth,
    yoy_dividend_growth
    FROM base_data
    WHERE yoy_revenue_growth IS NOT NULL
),
unified AS (
SELECT stock_ticker, financial_year_label, recency_rank,
       'Revenue growth'  AS metric_name, 1 AS metric_order,
       yoy_revenue_growth * 100 AS growth_value
FROM source

UNION ALL

SELECT stock_ticker, financial_year_label, recency_rank,
       'Net income growth' AS metric_name, 2 AS metric_order,
       yoy_net_income_growth * 100 AS growth_value
FROM source

UNION ALL

SELECT stock_ticker, financial_year_label, recency_rank,
       'Dividend growth'   AS metric_name, 3 AS metric_order,
       yoy_dividend_growth * 100 AS growth_value
FROM source
)

select * from unified

