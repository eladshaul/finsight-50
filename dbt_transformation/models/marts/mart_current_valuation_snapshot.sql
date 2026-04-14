{{ config(
    materialized='table',
    cluster_by = "sector"
) }}

WITH current_data AS (
    SELECT 
        s.*,
        SAFE_DIVIDE(s.current_market_cap, 1e9) AS current_market_cap_billions,
        p.sector,
        p.employee_count,
        -- חישוב מכפילים ברמת המניה (יותר מדויק)
        SAFE_DIVIDE(s.current_price, s.ttm_eps) AS pe_ratio,
        SAFE_DIVIDE(s.current_price, SAFE_DIVIDE(s.ttm_total_revenue, s.shares_outstanding)) AS ps_ratio,
        SAFE_DIVIDE(s.current_price, SAFE_DIVIDE(s.ttm_operating_cash_flow, s.shares_outstanding)) AS pcf_ratio,


        SAFE_DIVIDE(s.ttm_gross_profit, s.ttm_total_revenue) * 100 AS gross_margin,
        SAFE_DIVIDE(s.ttm_net_income, s.ttm_total_revenue) * 100 AS net_profit_margin,
        SAFE_DIVIDE(s.ttm_free_cash_flow, s.ttm_total_revenue) * 100 AS free_cash_flow_margin,

        SAFE_DIVIDE(s.ttm_total_revenue, s.shares_outstanding) AS revenue_per_share,
        s.ttm_eps as eps,
        SAFE_DIVIDE(s.ttm_free_cash_flow, s.shares_outstanding) AS free_cash_flow_per_share,

        SAFE_DIVIDE(s.ttm_net_income, s.stockholders_equity) * 100 AS roe,
        SAFE_DIVIDE(s.ttm_net_income, s.total_assets) * 100 AS roa,

        SAFE_DIVIDE(s.current_assets, s.current_liabilities) AS current_ratio,
        SAFE_DIVIDE(s.total_debt, s.stockholders_equity) AS debt_to_equity,

        SAFE_DIVIDE(s.ttm_total_revenue, s.total_assets) AS asset_turnover,

        SAFE_DIVIDE(s.ttm_dividends_paid, s.ttm_net_income) * 100 AS dividend_payout_ratio,
        SAFE_DIVIDE(SAFE_DIVIDE(s.ttm_dividends_paid, s.shares_outstanding), s.current_price) * 100 AS dividend_yield


    FROM {{ ref('int_quarterly_ttm_snapshot') }} s
    LEFT JOIN {{ ref('stg_company_profile') }} p 
        ON s.stock_ticker = p.stock_ticker
),

sector_averages AS (
    SELECT 
        *,
        -- ממוצעי סקטור נקיים (כולם על בסיס TTM נוכחי)
        PERCENTILE_CONT(pe_ratio, 0.5) OVER (PARTITION BY sector) AS sector_median_pe,
        PERCENTILE_CONT(ps_ratio, 0.5) OVER (PARTITION BY sector) AS sector_median_ps,
        PERCENTILE_CONT(pcf_ratio, 0.5) OVER (PARTITION BY sector) AS sector_median_pcf,

        AVG(gross_margin) OVER (PARTITION BY sector)  AS sector_avg_gross_margin,
        AVG(net_profit_margin) OVER (PARTITION BY sector)  AS sector_avg_net_margin,
        AVG(free_cash_flow_margin) OVER (PARTITION BY sector)  AS sector_avg_free_cash_flow_margin,

        AVG(revenue_per_share) OVER (PARTITION BY sector) AS sector_avg_revenue_per_share,
        AVG(eps) OVER (PARTITION BY sector) AS sector_avg_eps,
        AVG(free_cash_flow_per_share) OVER (PARTITION BY sector) AS sector_avg_free_cash_flow_per_share,

        AVG(roe) OVER (PARTITION BY sector)  AS sector_avg_roe,
        AVG(roa) OVER (PARTITION BY sector)  AS sector_avg_roa,

        AVG(current_ratio) OVER (PARTITION BY sector) AS sector_avg_current_ratio,
        AVG(debt_to_equity) OVER (PARTITION BY sector) AS sector_avg_debt_to_equity,

        AVG(asset_turnover) OVER (PARTITION BY sector) AS sector_avg_asset_turnover,

        AVG(dividend_payout_ratio) OVER (PARTITION BY sector)  AS sector_avg_dividend_payout_ratio,
        AVG(dividend_yield) OVER (PARTITION BY sector)  AS sector_avg_dividend_yield

    FROM current_data
)

SELECT * FROM sector_averages 