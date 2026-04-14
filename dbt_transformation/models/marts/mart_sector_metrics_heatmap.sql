{{ config(
    materialized='table',
    cluster_by = "sector"
) }}

WITH sector_aggregates AS (
    SELECT 
        s.*,
        p.sector,
        
        -- Quality metrics (higher is better)
        PERCENTILE_CONT(SAFE_DIVIDE(s.ttm_gross_profit, s.ttm_total_revenue), 0.5) 
            OVER (PARTITION BY p.sector) AS gross_margin_median,
        PERCENTILE_CONT(SAFE_DIVIDE(s.ttm_net_income, s.ttm_total_revenue), 0.5) 
            OVER (PARTITION BY p.sector) AS net_margin_median,
        PERCENTILE_CONT(SAFE_DIVIDE(s.ttm_free_cash_flow, s.ttm_total_revenue), 0.5) 
            OVER (PARTITION BY p.sector) AS fcf_margin_median,
        PERCENTILE_CONT(SAFE_DIVIDE(s.ttm_net_income, s.stockholders_equity), 0.5) 
            OVER (PARTITION BY p.sector) AS roe_median,
        
        -- Valuation metrics (lower is better for multiples)
        PERCENTILE_CONT(SAFE_DIVIDE(s.current_price, s.ttm_eps), 0.5) 
            OVER (PARTITION BY p.sector) AS pe_median,
        PERCENTILE_CONT(SAFE_DIVIDE(s.current_price, SAFE_DIVIDE(s.ttm_total_revenue, s.shares_outstanding)), 0.5) 
            OVER (PARTITION BY p.sector) AS ps_median,
            
        -- Health metrics (context-dependent)
        PERCENTILE_CONT(SAFE_DIVIDE(s.current_assets, s.current_liabilities), 0.5) 
            OVER (PARTITION BY p.sector) AS current_ratio_median,
        PERCENTILE_CONT(SAFE_DIVIDE(s.total_debt, s.stockholders_equity), 0.5) 
            OVER (PARTITION BY p.sector) AS debt_to_equity_median

    FROM {{ ref('int_quarterly_ttm_snapshot') }} s
    LEFT JOIN {{ ref('stg_company_profile') }} p 
        ON s.stock_ticker = p.stock_ticker
),

-- Pivot to long format for Looker Studio
unpivoted_metrics AS (
    SELECT 
        sector,
        'Gross Margin' AS metric,
        ROUND(gross_margin_median * 100, 2) AS value,
        'quality' AS metric_type,
        1 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
    
    UNION ALL
    SELECT 
        sector,
        'Net Margin' AS metric,
        ROUND(net_margin_median * 100, 2) AS value,
        'quality' AS metric_type,
        2 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
    
    UNION ALL
    SELECT 
        sector,
        'FCF Margin' AS metric,
        ROUND(fcf_margin_median * 100, 2) AS value,
        'quality' AS metric_type,
        3 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
    
    UNION ALL
    SELECT 
        sector,
        'ROE' AS metric,
        ROUND(roe_median * 100, 2) AS value,
        'quality' AS metric_type,
        4 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
    
    UNION ALL
    SELECT 
        sector,
        'P/E Ratio' AS metric,
        ROUND(pe_median, 2) AS value,
        'valuation' AS metric_type,
        5 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
    
    UNION ALL
    SELECT 
        sector,
        'P/S Ratio' AS metric,
        ROUND(ps_median, 2) AS value,
        'valuation' AS metric_type,
        6 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
    
    UNION ALL
    SELECT 
        sector,
        'Current Ratio' AS metric,
        ROUND(current_ratio_median, 2) AS value,
        'health' AS metric_type,
        7 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
    
    UNION ALL
    SELECT 
        sector,
        'Debt/Equity' AS metric,
        ROUND(debt_to_equity_median, 2) AS value,
        'health' AS metric_type,
        8 AS display_order
    FROM sector_aggregates
    WHERE sector IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sector ORDER BY sector) = 1
)

SELECT 
    sector,
    metric,
    value,
    metric_type,
    display_order
FROM unpivoted_metrics