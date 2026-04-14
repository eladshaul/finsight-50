import yfinance as yf
import pandas as pd
import time
import urllib.request

def get_sp500_top_50():
    print("--- Starting S&P 500 Data Extraction ---")
    

    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    
    try:

        with urllib.request.urlopen(req) as response:
            table = pd.read_html(response)
        
        df_sp500 = table[0]
        tickers = df_sp500['Symbol'].tolist()
        
    except Exception as e:
        print(f"Error fetching Wikipedia table: {e}")
        return None


    tickers = [t.replace('.', '-') for t in tickers]
    
    all_companies_data = []
    
    print(f"Fetching info for {len(tickers)} companies. Please wait...")
    

    for i, ticker in enumerate(tickers):
        try:
            t = yf.Ticker(ticker)
            info = t.info
            

            data = {
                'ticker': ticker,
                'displayName': info.get('longName'),
                'state': info.get('state'),
                'city': info.get('city'),
                'country': info.get('country'),
                'industry': info.get('industry'),
                'industryKey': info.get('industryKey'),
                'industryDisp': info.get('industryDisplay'),
                'sector': info.get('sector'),
                'sectorKey': info.get('sectorKey'),
                'sectorDisp': info.get('sectorDisplay'),
                'marketCap': info.get('marketCap', 0)
            }
            all_companies_data.append(data)
            

            if (i + 1) % 50 == 0:
                print(f"Processed {i + 1}/500 companies...")
                
        except Exception as e:
            print(f"Could not fetch {ticker}: {e}")
            continue


    full_df = pd.DataFrame(all_companies_data)
    

    full_df = full_df.drop_duplicates(subset=['displayName'], keep='first')
    

    top_50_df = full_df.sort_values(by='marketCap', ascending=False).head(50)
    
    return top_50_df

if __name__ == "__main__":
    start_time = time.time()
    
    final_table = get_sp500_top_50()
    
    if final_table is not None:

        print("\n--- Top 50 UNIQUE S&P 500 Companies by Market Cap ---")
        print(final_table[['ticker', 'displayName', 'marketCap', 'sector']].head(10))
        

        final_table.to_csv("top_50_sp500_info.csv", index=False)
        print(f"\nSaved {len(final_table)} unique companies to top_50_sp500_info.csv")
    
    end_time = time.time()
    print(f"Execution time: {round((end_time - start_time) / 60, 2)} minutes")