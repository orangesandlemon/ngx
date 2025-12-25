# === File: sweden/analyser_se.py ===

import sqlite3
import pandas as pd
from datetime import datetime
from datetime import timedelta



# === CONFIG ===

DB_PATH = "data/us_equities.db"

# Skip weekends
#if datetime.today().weekday() >= 5:
#print("🛑 Market closed (weekend). Exiting.")
 #  exit()

# === Connect and Load ===
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "data/us_equities.db"

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by=["name", "date"])

# === Price Change Calculations ===
df["change"] = ((df["close"] - df["open"]) / df["open"]) * 100
df["change"] = df["change"].round(2)

df["change_down"] = df.apply(
    lambda row: round(((row["close"] - row["open"]) / row["open"]) * 100, 2)
    if row["close"] < row["open"] else None,
    axis=1
)

# === Candlestick Shape Analysis ===
def detect_hammer(row):
    body = abs(row["close"] - row["open"])
    lower_wick = row["low"] - min(row["close"], row["open"])
    upper_wick = row["high"] - max(row["close"], row["open"])

    if body == 0: return None

    # Hammer (Potential Buy): small body, long lower wick
    if lower_wick > 2 * body and upper_wick < body:
        return "🟢 Hammer (Potential Buy)"

    # Inverted Hammer (Potential Sell): small body, long upper wick
    if upper_wick > 2 * body and lower_wick < body:
        return "🔴 Inverted Hammer (Potential Sell)"

    return None

df["pattern"] = df.apply(detect_hammer, axis=1)

# === Signal Detection ===
# === Signal Detection ===
signals = []
latest_day = df["date"].max()
start_day = latest_day - timedelta(days=1)
filtered_df = df[df["date"] >= start_day]

for _, row in filtered_df.iterrows():
    name = row["name"]
    date = row["date"].strftime("%Y-%m-%d")
    change = row["change"]
    change_down = row["change_down"]
    volume = row["volume"]

    # Wick + Body analysis
    body = abs(row["close"] - row["open"])
    lower_wick = row["low"] - min(row["close"], row["open"])
    upper_wick = row["high"] - max(row["close"], row["open"])

    signal = None
    reasons = []

    # === Buy Conditions ===
    is_hammer = lower_wick > 2 * body and upper_wick < body
    if (change > 3 and volume > 100000) or (is_hammer and change > 0 and volume > 50000):
        signal = "⚡ Strong Buy Setup"
        if change > 3:
            reasons.append("Price Up > 3%")
        if volume > 100000:
            reasons.append("High Volume")
        if is_hammer:
            reasons.append("Hammer Pattern")

    elif change > 1:
        signal = "👀 Watchlist"
        reasons.append("Mild Uptrend")

    # === Sell Conditions ===
    is_inverted_hammer = upper_wick > 2 * body and lower_wick < body
    if change_down and change_down < -3 and volume > 100000:
        signal = "⚡ Strong Sell Setup"
        reasons.append("Price Down > 3%")
        reasons.append("High Volume")
        if is_inverted_hammer:
            reasons.append("Inverted Hammer Pattern")

    elif change_down and change_down < -1:
        signal = "👀 Watchlist_Sell"
        reasons.append("Mild Downtrend")
        if is_inverted_hammer:
            reasons.append("Inverted Hammer Pattern")

    # === Final Signal ===
    if signal:
        signals.append({
            "name": name,
            "date": date,
            "change": change,
            "change_down": change_down,
            "volume": volume,
            "signal": signal,
            "reasons": ", ".join(reasons)
        })


# === Save Signals ===
if signals:
    signals_df = pd.DataFrame(signals)

    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_2 (
                name TEXT,
                date TEXT,
                change REAL,
                change_down REAL,
                volume INTEGER,
                signal TEXT,
                reasons TEXT,
                PRIMARY KEY (name, date)
            )
        """)

        # Drop duplicates before inserting
        for _, row in signals_df.iterrows():
            conn.execute("DELETE FROM signals_2 WHERE name = ? AND date = ?", (row["name"], row["date"]))

        signals_df.to_sql("signals_2", conn, if_exists="append", index=False)

    print(f"✅ Saved {len(signals)} signals from {start_day.strftime('%Y-%m-%d')} to {latest_day.strftime('%Y-%m-%d')}")
else:
    print("ℹ️ No signals found.")

conn.close()

