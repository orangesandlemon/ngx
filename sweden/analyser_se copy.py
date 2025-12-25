# === File: sweden/analyser_se.py ===

import sqlite3
import pandas as pd
from datetime import datetime

# === CONFIG ===
DB_PATH = "data/omx_equities.db"

# Skip weekends
#if datetime.today().weekday() >= 5:
#print("🛑 Market closed (weekend). Exiting.")
 #  exit()

# === Connect and Load ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by=["name", "date"])

# === Add % Change ===
df["change"] = ((df["close"] - df["open"]) / df["open"]) * 100
df["change"] = df["change"].round(2)

# === Signal Detection ===
signals = []
latest_day = df["date"].max()
today_df = df[df["date"] == latest_day]

for _, row in today_df.iterrows():
    name = row["name"]
    change = row["change"]
    volume = row["volume"]
    signal = None
    reasons = []

    # Simple logic
    if change > 3 and volume > 100000:
        signal = "⚡ Strong Buy Setup"
        reasons.append("Price Up > 3%")
        reasons.append("High Volume")

    elif change > 1:
        signal = "👀 Watchlist"
        reasons.append("Mild Uptrend")

    if signal:
        signals.append({
            "name": name,
            "date": latest_day.strftime("%Y-%m-%d"),
            "change": change,
            "volume": volume,
            "signal": signal,
            "reasons": ", ".join(reasons)
        })

# === Save to DB ===
if signals:
    signals_df = pd.DataFrame(signals)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_se (
                name TEXT,
                date TEXT,
                change REAL,
                volume INTEGER,
                signal TEXT,
                reasons TEXT,
                PRIMARY KEY (name, date)
            )
        """)
        conn.execute("DELETE FROM signals_se WHERE date = ?", (latest_day.strftime("%Y-%m-%d"),))
        signals_df.to_sql("signals_se", conn, if_exists="append", index=False)
    print(f"✅ Saved {len(signals)} signals for {latest_day.strftime('%Y-%m-%d')}")
else:
    print("ℹ️ No signals today.")

conn.close()
