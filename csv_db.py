import pandas as pd
import sqlite3

csv_path = "stocks_with_main_and_subsector.csv"
df_baseline = pd.read_csv(csv_path)
df_baseline["symbol"] = df_baseline["symbol"].str.strip().str.lower()

conn = sqlite3.connect("data/ngx_equities.db")
cursor = conn.cursor()

for _, row in df_baseline.iterrows():
    name = row["symbol"]
    sector = row["main_sector"]
    subsector = row["sub_sector"]

    cursor.execute("""
        UPDATE equities
        SET
            main_sector = ?,
            sub_sector = ?
        WHERE LOWER(name) LIKE ?
    """, (sector, subsector, f"%{name}%"))

conn.commit()
conn.close()

print("✅ Successfully updated today's equities with sector info.")
