import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# --- Load CSV ---
csv_file = "Corrected_Symbols_and_Last_Prices.csv"  # path to your CSV
df = pd.read_csv(csv_file)

# Ensure numeric
df["Last Price"] = pd.to_numeric(
    df["Last Price"].astype(str).str.replace(",", ""), errors="coerce"
)

# --- Define "today" (weekday; weekend rolls back to Friday) ---
tz = ZoneInfo("Africa/Lagos")  # or "Europe/Stockholm"
today = datetime.now(tz)

# If Saturday (5), subtract 1 day; if Sunday (6), subtract 2 days
if today.weekday() == 5:
    today -= timedelta(days=1)
elif today.weekday() == 6:
    today -= timedelta(days=2)

today_str = today.strftime("%Y-%m-%d")

# --- Connect to DB ---
db_path = "data/ngx_equities.db"  # adjust if different
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# --- Update DB for today's date only ---
for _, row in df.iterrows():
    symbol = row["Symbol"]
    last_price = row["Last Price"]

    if pd.notnull(last_price):
        cursor.execute(
            f"""
            UPDATE equities
            SET close = ?
            WHERE name = ? AND date = ?
        """,
            (last_price, symbol, today_str),
        )

# Commit and close
conn.commit()
conn.close()

print(f"✅ Close prices updated successfully for {today_str}.")
