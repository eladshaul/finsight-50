import dlt
import yfinance as yf
import pandas as pd
import os
import json
import time
credentials = json.loads(os.environ['GCP_CREDS'])
is_backfill = os.environ['IS_BACKFILL'].lower() == 'true'
dataset_name = os.environ['BQ_DATASET']
bucket_url = f"gs://{os.environ['GCS_BUCKET']}"
TICKERS = pd.read_csv('top_50_tickers.csv')['ticker'].tolist()
def get_financial_stmt(ticker_obj, stmt_type, is_annual):
    try:
        if stmt_type == 'income':
            df = ticker_obj.income_stmt if is_annual else ticker_obj.quarterly_income_stmt
        elif stmt_type == 'balance':
            df = ticker_obj.balance_sheet if is_annual else ticker_obj.quarterly_balance_sheet
        else:
            df = ticker_obj.cashflow if is_annual else ticker_obj.quarterly_cashflow
        if df is None or df.empty: 
            return pd.DataFrame()
        
        df = df.T
        if not is_backfill:
            df = df.head(1)
        
        return df.reset_index().rename(columns={'index': 'report_date'})
    except Exception as e:
        print(f"Error fetching {stmt_type} for ticker: {e}")
        return pd.DataFrame()
@dlt.resource(name="fact_history", write_disposition="merge", primary_key=("ticker", "Date"))
def stock_prices():
    period = "4y" if is_backfill else "1mo"
    for ticker in TICKERS:
        data = yf.download(ticker, period=period, interval="1d", progress=False)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] for col in data.columns]
        data.columns = [str(col) for col in data.columns]
        data = data.reset_index()
        data['Date'] = data['Date'].astype(str)   # ensure Date is serializable too
        data['ticker'] = ticker
        yield data.to_dict(orient="records")
        time.sleep(0.2)
@dlt.resource(name="company_info_dim", write_disposition="replace")
def company_info():
    for ticker in TICKERS:
        try:
            info = yf.Ticker(ticker).info
            essential_keys = ['shortName', 'sector', 'industry', 'fullTimeEmployees', 'longBusinessSummary', 'city', 'country', 'marketCap']
            clean = {k: info[k] for k in essential_keys if k in info}
            clean['ticker'] = ticker
            yield clean
            time.sleep(0.5)
        except:
            continue
def create_stmt_resource(stmt_name, stmt_type, is_annual):
    resource_name = f"fact_{stmt_name}_{'annual' if is_annual else 'quarter'}"
    @dlt.resource(name=resource_name, write_disposition="merge", primary_key=("ticker", "report_date"))
    def financial_resource(): 
        for ticker in TICKERS:
            t = yf.Ticker(ticker)
            df = get_financial_stmt(t, stmt_type, is_annual)
            if not df.empty:
                df['ticker'] = ticker
                yield df.to_dict(orient="records")
            time.sleep(0.8)
    return financial_resource
if __name__ == "__main__":
    resources = [
        stock_prices(),
        company_info(),
        create_stmt_resource("income_statement", "income", True)(),
        create_stmt_resource("income_statement", "income", False)(),
        create_stmt_resource("balance_sheet", "balance", True)(),
        create_stmt_resource("balance_sheet", "balance", False)(),
        create_stmt_resource("cash_flow", "cash_flow", True)(),
        create_stmt_resource("cash_flow", "cash_flow", False)()
    ]
    pipeline = dlt.pipeline(
        pipeline_name="yfinance_full_load",
        destination=dlt.destinations.bigquery(credentials=credentials, project_id=os.environ['GCP_PROJECT_ID']),
        staging=dlt.destinations.filesystem(bucket_url=bucket_url, credentials=credentials),
        dataset_name=dataset_name
    )
    load_info = pipeline.run(resources)
    print(load_info)
