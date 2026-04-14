{{ config(
    materialized='table',
    cluster_by = "stock_ticker"
) }}

WITH ranked_quarters AS (

    SELECT 
        inc.stock_ticker,
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
        cf.debt_repaid,
        ROW_NUMBER() OVER (PARTITION BY inc.stock_ticker ORDER BY inc.report_date DESC) as rn
        
    FROM {{ ref('stg_income_statement_quarterly') }} inc
    inner join {{ ref('stg_balance_sheet_quarterly') }} bal 
        ON inc.stock_ticker = bal.stock_ticker AND extract(YEAR FROM inc.report_date) = extract(YEAR FROM bal.report_date) AND extract(QUARTER FROM inc.report_date) = extract(QUARTER FROM bal.report_date)
    inner join {{ ref('stg_cash_flow_quarterly') }} cf 
        ON inc.stock_ticker = cf.stock_ticker AND extract(YEAR FROM inc.report_date) = extract(YEAR FROM cf.report_date) AND extract(QUARTER FROM inc.report_date) = extract(QUARTER FROM cf.report_date)
    WHERE inc.report_type = 'quarterly'
),

ttm_flows AS (

    SELECT 
        stock_ticker,
        SUM(total_revenue) as ttm_total_revenue,
        SUM(gross_profit) as ttm_gross_profit,
        SUM(net_income) as ttm_net_income,
        SUM(operating_expenses) as ttm_operating_expenses,
        SUM(operating_income_ebit) as ttm_operating_income_ebit,
        SUM(interest_expense) as ttm_interest_expense,
        SUM(rd_expenses) as ttm_rd_expenses,
        SUM(sga_expenses) as ttm_sga_expenses,
        SUM(free_cash_flow) as ttm_free_cash_flow,
        SUM(operating_cash_flow) as ttm_operating_cash_flow,
        SUM(dividends_paid) as ttm_dividends_paid,
        SUM(eps) as ttm_eps
    FROM ranked_quarters
    WHERE rn <= 4
    GROUP BY stock_ticker

    HAVING COUNT(rn) = 4
),

latest_balance AS (
  
    SELECT 
        stock_ticker,
        report_date as latest_report_date,
        shares_outstanding,
        stockholders_equity,
        total_debt,
        total_assets,
        cash_and_cash,
        current_assets,
        current_liabilities
    FROM ranked_quarters
    WHERE rn = 1
),

latest_price AS (

    SELECT 
        stock_ticker,
        measurement_date as latest_price_date,
        close_price as current_price
    FROM {{ ref('stg_stock_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stock_ticker ORDER BY measurement_date DESC) = 1
)


SELECT 
    lb.stock_ticker,
    lb.latest_report_date,
    lp.latest_price_date,
    lp.current_price,
    lb.shares_outstanding,
    lb.stockholders_equity,
    lb.total_debt,
    lb.total_assets,
    lb.cash_and_cash,
    lb.current_assets,
    lb.current_liabilities,
    tf.ttm_total_revenue,
    tf.ttm_gross_profit,
    tf.ttm_net_income,
    tf.ttm_operating_expenses,
    tf.ttm_operating_income_ebit,
    tf.ttm_interest_expense,
    tf.ttm_rd_expenses,
    tf.ttm_sga_expenses,
    tf.ttm_free_cash_flow,
    tf.ttm_operating_cash_flow,
    tf.ttm_dividends_paid,
    tf.ttm_eps,
    (lb.shares_outstanding * lp.current_price) AS current_market_cap 
FROM latest_balance lb
INNER JOIN ttm_flows tf ON lb.stock_ticker = tf.stock_ticker
LEFT JOIN latest_price lp ON lb.stock_ticker = lp.stock_ticker