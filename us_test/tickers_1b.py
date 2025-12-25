# save_tickers_above_1b.py
import yfinance as yf
import pandas as pd
import requests

def fetch_us_tickers():
    urls = {
      'nasdaq': "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
      'other':  "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
    }
    dfs = {}
    for name, url in urls.items():
        txt = requests.get(url).text.splitlines()
        dfs[name] = pd.DataFrame([r.split('|') for r in txt[1:-1]])
    return pd.concat(dfs.values(), ignore_index=True)

df_all = fetch_us_tickers()
symbols = df_all[0].tolist()  # the ticker column
print(f"Found {len(symbols)} total symbols")

def filter_large_caps(symbols, min_cap=1_000_000_000):
    large = []
    for sym in symbols:
        try:
            info = yf.Ticker(sym).info
            mcap = info.get('marketCap', 0)
            if mcap and mcap >= min_cap:
                large.append(sym)
        except Exception:
            continue
    return large


def update_largecap_csv(filepath="us_largecap_tickers.csv"):
    symbols = fetch_us_tickers()
    large = filter_large_caps(symbols)
    pd.DataFrame({'symbol': large}).to_csv(filepath, index=False)
    print(f"✅ Saved {len(large)} tickers to {filepath}")
