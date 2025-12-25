# === File: sweden/scraper_yahoo.py ===

import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import os

# === CONFIG ===
DB_PATH = "data/us_equities.db"
TICKERS = [
    # 🔹 ETFs (Highly Liquid)
    "SPY",    # S&P 500 ETF
    "QQQ",    # Nasdaq-100 ETF
    "IWM",    # Russell 2000 ETF
    "DIA",    # Dow Jones ETF
    "VIX",    # Volatility Index

    # 🔹 Mega Cap Stocks
    "TSLA",   # Tesla
    "AAPL",   # Apple
    "NVDA",   # NVIDIA
    "AMD",    # AMD
    "AMZN",   # Amazon
    "MSFT",   # Microsoft
    "META",   # Meta Platforms
    "GOOGL",  # Alphabet
    "DKNG",    # DraftKings - sports betting momentum
    "RBLX",    # Roblox - young retail audience
    "CHPT",    # ChargePoint - EV hype, volatile
    "LCID",    # Lucid Motors - EV play, low float
    "UPST",    # Upstart - high volatility, squeezes
    "HOOD",    # Robinhood - irony: retail trades retail
    "BB",      # BlackBerry - meme nostalgia
    "SNAP",    # Snap Inc - tech + youth
    "PYPL",    # PayPal - fintech bounce plays
    "SQ",      # Block Inc - fintech + volatility
    "ABNB",    # Airbnb - heavy post-earnings movement

    # 🔹 Liquid Mid-Large Caps
    "NFLX",   # Netflix
    "BA",     # Boeing
    "COIN",   # Coinbase
    "BABA",   # Alibaba
    "INTC",   # Intel
    "PLTR",   # Palantir
    "XOM",    # ExxonMobil
    "CVX",    # Chevron
    "JPM",    # JPMorgan
    "WMT",    # Walmart
    "COST",   # Costco
    "SMCI",
    "RIOT",   # Riot Platforms (crypto momentum)
    "MARA",   # Marathon Digital (crypto retail play)
    "SOFI",   # SoFi Technologies (popular with retail)
    "FUBO",   # FuboTV (low float, squeezes)
    "GME",    # GameStop (classic retail target)
    "AMC",    # AMC Entertainment (high volume action)
    "IONQ",   # IonQ Inc (AI buzz + volatility)
    "AI",     # C3.ai (AI trend + retail interest)
    "NVOS",   # Novo Integrated Sciences (cheap stock, volatile)
    "TQQQ"   # ProShares Ultra QQQ (leveraged ETF, fast setups)
    
]


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
start_date =  last_trading_day.strftime("%Y-%m-%d")
end_date = (last_trading_day + timedelta(days=175)).strftime("%Y-%m-%d")


# === Prepare DB ===
os.makedirs("sweden/data", exist_ok=True)
conn = sqlite3.connect(DB_PATH)

with conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equities (
            name TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (name, date)
        )
    """)

# === Main Loop ===
all_data = []

for ticker in TICKERS:
    print(f"[+] Downloading {ticker} from Yahoo Finance...")
    df = yf.download(ticker, start=start_date, end=end_date, interval="1d", auto_adjust=True)

    if df.empty:
        print(f"[!] No data for {ticker}")
        continue

    df.reset_index(inplace=True)
    df["name"] = ticker
    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = df[["name", "date", "Open", "High", "Low", "Close", "Volume"]]
    df.columns = ["name", "date", "open", "high", "low", "close", "volume"]
    df = df.dropna()

    all_data.append(df)

# === Insert to DB
if all_data:
    combined = pd.concat(all_data, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined = combined.drop_duplicates(subset=["name", "date"])

    with conn:
        # Delete just for this scraped date
        for d in combined["date"].unique():
            conn.execute("DELETE FROM equities WHERE date = ?", (d,))
        combined.to_sql("equities", conn, if_exists="append", index=False)

    print(f"[✓] Stored {len(combined)} total rows for {len(TICKERS)} tickers.")
    print(f"[✓] Inserted {len(combined)} rows into 'equities' table.")
    print(f"[✓] Done for {start_date}")
else:
    print("[x] No data to insert.")

conn.close()