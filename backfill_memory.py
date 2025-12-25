from analyser_core import run_analyzer_on_dataframe
import sqlite3
import pandas as pd

DB_PATH = "data/ngx_equities.db"

# Step 1: Get all unique dates
with sqlite3.connect(DB_PATH) as conn:
    all_dates = pd.read_sql("SELECT DISTINCT date FROM equities ORDER BY date ASC", conn)["date"].tolist()

print(f"🔁 Replaying analyzer for {len(all_dates)} trading days...")

for date in all_dates:
    print(f"\n📅 Processing {date}...")

    try:
        # Fresh connection per day
        daily_conn = sqlite3.connect(DB_PATH)
        daily_df = pd.read_sql("SELECT * FROM equities WHERE date = ?", daily_conn, params=(date,))
        daily_conn.close()

        if daily_df.empty:
            print("⚠️  No data for this day, skipping.")
            continue

        daily_df["date"] = pd.to_datetime(daily_df["date"])
        
        # ✅ Fresh connection passed inside analyzer (not shared)
        run_analyzer_on_dataframe(daily_df, DB_PATH, skip_summary=True)

    except Exception as e:
        print(f"❌ Error running analyzer on {date}: {e}")
