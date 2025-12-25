# intel_comparator.py
import sqlite3
import pandas as pd
from datetime import datetime

# === CONFIG ===
DB_PATH = "data/us_equities.db"
TABLE_30D = "weekly_intel_100"
TABLE_10D = "weekly_intel_short_100"
DATE_FIELD = "date_generated"

# === Connect to DB ===
conn = sqlite3.connect(DB_PATH)

# === Load both tables ===
df_30 = pd.read_sql(
    f"SELECT * FROM {TABLE_30D} WHERE score >= 1 and avg_change_1 > avg_change_0 and avg_vol_1 > avg_vol_0 and close_end_1 > close_start_1",
    conn,
)
df_10 = pd.read_sql(
    f"SELECT * FROM {TABLE_10D} WHERE score >= 1 and avg_change_1 > avg_change_0 and avg_vol_1 > avg_vol_0 and close_end_1 > close_start_1",
    conn,
)

conn.close()

# === Parse dates and keep only latest per stock ===
df_30[DATE_FIELD] = pd.to_datetime(df_30[DATE_FIELD])
df_10[DATE_FIELD] = pd.to_datetime(df_10[DATE_FIELD])

# Latest 30D date per stock
df_30_latest = df_30.sort_values(DATE_FIELD).groupby("name").tail(1)
df_10_latest = df_10.sort_values(DATE_FIELD).groupby("name").tail(1)

# === Merge for comparison ===
df_merged = pd.merge(df_30_latest, df_10_latest, on="name", suffixes=("_30", "_10"))


# === Define status logic ===
def analyze_stock(row):
    change_30 = row.get("avg_change_1_30", 0)
    change_10 = row.get("avg_change_1_10", 0)
    vol_30 = row.get("volume_1_30", 0)
    vol_10 = row.get("volume_1_10", 0)
    close_30 = row.get("close_end_1_30", 0)
    close_10 = row.get("close_end_1_10", 0)
    start_30 = row.get("close_start_1_30", 0)
    start_10 = row.get("close_start_1_10", 0)

    status = []
    notes = []

    # === Price Momentum ===
    if change_30 > 0 and change_10 > 0:
        status.append("✅ Strong Uptrend")
    elif change_30 > 0 and change_10 < 0:
        status.append("⚠️ 10D Weakness, 30D Strong")
        notes.append("Short-term pullback. Watch for bounce or fail.")
    elif change_30 < 0 and change_10 > 0:
        status.append("🔁 Possible Reversal")
        notes.append("10D showing recovery while 30D still down.")
    elif change_30 < 0 and change_10 < 0:
        status.append("❌ Downtrend")
        notes.append("Both windows declining. Avoid or exit.")
    else:
        status.append("😐 Mixed Trend")

    # === Volume Check ===
    vol_ratio = (vol_10 / vol_30) if vol_30 else 0
    if vol_ratio > 1.3:
        notes.append("Unusual short-term volume spike")
    elif vol_ratio < 0.7:
        notes.append("Volume tapering. Possibly quiet accumulation or weakness.")

    # === Price Level ===
    if close_10 < start_10 and close_30 > start_30:
        notes.append("Short-term rejection despite bullish 30D")
    if close_10 > start_10 and close_30 < start_30:
        notes.append("Early breakout after 30D weakness")

    return {
        "name": row["name"],
        "change_30": round(change_30, 3),
        "change_10": round(change_10, 3),
        "vol_30": int(vol_30),
        "vol_10": int(vol_10),
        "start_10": round(start_10, 2),
        "close_10": round(close_10, 2),
        "status": "; ".join(status),
        "notes": "; ".join(notes),
    }


# === Run Analysis ===
report = pd.DataFrame([analyze_stock(row) for _, row in df_merged.iterrows()])

# === Output ===
print(report.head(20))  # Show sample

# === Save to file ===
report.to_csv("intel_comparison_report_100.csv", index=False)
print("✅ Report saved to intel_comparison_report_100.csv")


# Connect to your SQLite database
conn = sqlite3.connect("data/us_equities.db")  # adjust path as needed

# Save to table; this will create the table if it doesn't exist
report.to_sql("intel_comparison_100", conn, if_exists="replace", index=False)

conn.close()
print("✅ Report saved to table 'intel_comparison_100' in us_equities.db")
