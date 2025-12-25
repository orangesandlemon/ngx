# === File: sweden/scraper_yahoo.py ===

import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import os
import requests

# === CONFIG ===
DB_PATH = "data/us_equities.db"

def fetch_us_tickers_from_under25_json(filepath="tickers_25.json"):
    import json
    with open(filepath, "r") as f:
        data = json.load(f)

    tickers = [entry["name"] for entry in data if "name" in entry]
    return tickers


# === Fetch and Filter Tickers ===
print("🔎 Fetching tickers...")
all_symbols = fetch_us_tickers_from_under25_json("tickers_25.json")
print(f"Found {len(all_symbols)} raw tickers.")

TICKERS = all_symbols
print(f"✅ Filtered to {len(TICKERS)} tickers under $100 and over $500M cap.")


# === Get Last Valid Trading Day ===
def get_last_trading_day():
    today = datetime.today()
    if today.weekday() == 5:  # Saturday
        return today - timedelta(days=1)  # Friday
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)  # Friday
    else:
        return today

last_trading_day = get_last_trading_day()
start_date =last_trading_day.strftime("%Y-%m-%d") #"2025-04-01" 
end_date = (last_trading_day + timedelta(days=1)).strftime("%Y-%m-%d")#"2025-06-27"


# === Prepare DB ===
#os.makedirs("sweden/data", exist_ok=True)
conn = sqlite3.connect(DB_PATH)

with conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equities_100 (
            name TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            market_cap INTEGER,
            previous_close REAL,
            PRIMARY KEY (name, date)
        )


    """)

# === Main Loop ===
all_data = []

for ticker in TICKERS:
    try:
        info = yf.Ticker(ticker).info
        mcap = info.get("marketCap", None)
        price = info.get("regularMarketPrice", None)
        prev_close = info.get("previousClose", None)

        # 🧠 Filter: skip if no cap or price, or price > 100, or cap < 500M
        if not mcap or not price or mcap < 500_000_000 or price > 100:
            print(f"[⛔] Skipping {ticker} — Price: {price}, Market Cap: {mcap}")
            continue

        print(f"[+] Downloading {ticker} from Yahoo Finance...")
        df = yf.download(ticker, start=start_date, end=end_date, interval="1d", auto_adjust=True)

        if df.empty:
            with open("failed_tickers.txt", "a") as f:
                f.write(f"{ticker}\n")
            print(f"[!] No data for {ticker}")
            continue

    except Exception as e:
        with open("failed_tickers.txt", "a") as f:
            f.write(f"{ticker}\n")
        print(f"[ERROR] {ticker}: {e}")
        continue

    df.reset_index(inplace=True)
    df["name"] = ticker
    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = df[["name", "date", "Open", "High", "Low", "Close", "Volume"]]
    df.columns = ["name", "date", "open", "high", "low", "close", "volume"]
    df = df.dropna()

    df["market_cap"] = mcap  # add market cap to each row
    df["previous_close"] = prev_close

    all_data.append(df)


# === Insert to DB
if all_data:
    combined = pd.concat(all_data, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined = combined.drop_duplicates(subset=["name", "date"])

    with conn:
        # Delete just for this scraped date
        for d in combined["date"].unique():
            conn.execute("DELETE FROM equities_100 WHERE date = ?", (d,))
        combined.to_sql("equities_100", conn, if_exists="append", index=False)

    print(f"[✓] Stored {len(combined)} total rows for {len(TICKERS)} tickers.")
    print(f"[✓] Inserted {len(combined)} rows into 'equities_100' table.")
    print(f"[✓] Done for {start_date}")
else:
    print("[x] No data to insert.")

conn.close()