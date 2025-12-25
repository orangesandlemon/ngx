# options_pricer.py — Prefer 2nd Friday, fallback to next Friday, only update signals from last 3 days
import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time

DB_PATH = "data/us_equities.db"
MAX_OPTION_PRICE = 1.50

def get_upcoming_fridays(start_date, count=2):
    fridays = []
    date = start_date
    while len(fridays) < count:
        if date.weekday() == 4:
            fridays.append(date)
        date += timedelta(days=1)
    return [d.strftime("%Y-%m-%d") for d in fridays]

# === Load signals ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM signals_us", conn)
df["date"] = pd.to_datetime(df["date"])
today = datetime.now().date()

df["option_entry"] = df["option_entry"].fillna(0)

# Only update rows from today, yesterday, or 2 days ago
targets = df[
    (df["action"].isin(["BUY CALL", "BUY PUT", "WATCH"])) &
    (df["option_entry"] <= 0.5) &
    (df["date"].dt.date >= today - timedelta(days=2))
]

print(f"🎯 Found {len(targets)} recent signal(s) to update.")

updates = []

# === Get next 2 Fridays ===
next_friday, second_friday = get_upcoming_fridays(today)

for _, row in targets.iterrows():
    symbol = row["name"]
    action = row["action"]
    close_price = row["close"]
    strike_hint = round(close_price * (0.98 if "CALL" in action else 1.02), 2)

    try:
        ticker = yf.Ticker(symbol)
        expiries = ticker.options

        expiry = None
        valid_expiry = None

        # Try second Friday first
        if second_friday in expiries:
            expiry = second_friday
            chain = ticker.option_chain(expiry)
            opt_df = chain.calls if "CALL" in action else chain.puts

            if "CALL" in action:
                opt_df = opt_df[opt_df["strike"] > close_price]
            else:
                opt_df = opt_df[opt_df["strike"] < close_price]

            opt_df = opt_df[(opt_df["bid"] > 0) & (opt_df["bid"] <= MAX_OPTION_PRICE)]

            if not opt_df.empty:
                opt_df["distance"] = abs(opt_df["strike"] - strike_hint)
                best = opt_df.sort_values("distance").iloc[0]
                entry = round(best["bid"], 2)
                target = round(entry * 1.8, 2)
                valid_expiry = expiry
                print(f"📦 {symbol} {action} → Entry: ${entry:.2f} Target: ${target:.2f} Expiry: {expiry}")

        # Fallback to next Friday
        if valid_expiry is None and next_friday in expiries:
            expiry = next_friday
            chain = ticker.option_chain(expiry)
            opt_df = chain.calls if "CALL" in action else chain.puts

            if "CALL" in action:
                opt_df = opt_df[opt_df["strike"] > close_price]
            else:
                opt_df = opt_df[opt_df["strike"] < close_price]

            opt_df = opt_df[(opt_df["bid"] > 0) & (opt_df["bid"] <= MAX_OPTION_PRICE)]

            if not opt_df.empty:
                opt_df["distance"] = abs(opt_df["strike"] - strike_hint)
                best = opt_df.sort_values("distance").iloc[0]
                entry = round(best["bid"], 2)
                target = round(entry * 1.8, 2)
                valid_expiry = expiry
                print(f"📦 {symbol} {action} → Entry: ${entry:.2f} Target: ${target:.2f} Expiry: {expiry}")

        if valid_expiry is None:
            expiry = second_friday
            raise ValueError("No valid options found")

    except Exception as e:
        entry = round(close_price * 0.02, 2)
        target = round(entry * 1.8, 2)
        expiry = second_friday
        print(f"⚠️  {symbol}: Estimated entry @ ${entry:.2f}, expiry: {expiry} — reason: {e}")

    updates.append({
        "name": symbol,
        "date": row["date"].strftime("%Y-%m-%d"),
        "option_entry": entry,
        "option_target": target,
        "option_expiry": expiry
    })

    time.sleep(1.5)

# === Update DB ===
cursor = conn.cursor()
for u in updates:
    cursor.execute("""
        UPDATE signals_us
        SET option_entry = ?, option_target = ?, option_expiry = ?
        WHERE name = ? AND date = ?
    """, (u["option_entry"], u["option_target"], u["option_expiry"], u["name"], u["date"]))

conn.commit()
conn.close()
print(f"\n✅ Updated {len(updates)} recent signal(s) with real or fallback pricing.")
