import sqlite3
import pandas as pd
from datetime import datetime

# === CONFIG ===
DB_PATH = "data/ngx_equities.db"  # Adjust path as needed

# === STEP 1: Connect to DB ===
conn = sqlite3.connect(DB_PATH)

# === STEP 2: Get Latest Date ===
latest_date_query = "SELECT MAX(date) FROM equities"
latest_date = pd.read_sql_query(latest_date_query, conn).iloc[0, 0]

# === STEP 3: Pull Today's Data ===
query_today = f"""
SELECT name, date, value, open, close
FROM equities
WHERE date = '{latest_date}'
"""
df_today = pd.read_sql_query(query_today, conn)

# === STEP 4: Get Previous Close ===
query_prev = f"""
SELECT name, close AS previous_close
FROM equities
WHERE date = (
    SELECT MAX(date) FROM equities WHERE date < '{latest_date}'
)
"""
df_prev = pd.read_sql_query(query_prev, conn)

# === STEP 5: Merge Previous Close & Rank ===
df_today = df_today.merge(df_prev, on="name", how="left")
df_today["value_rank"] = (
    df_today["value"].rank(method="min", ascending=False).astype(int)
)

# === STEP 6: Create Output Table (if not exists) ===
conn.execute(
    """
CREATE TABLE IF NOT EXISTS value_rank_history (
    name TEXT,
    date TEXT,
    value REAL,
    value_rank INTEGER,
    open REAL,
    close REAL,
    previous_close REAL
)
"""
)

# === STEP 7: Insert Data ===
df_today.to_sql("value_rank_history", conn, if_exists="append", index=False)

# === Done ===
print(f"[✓] Value ranks for {latest_date} stored in 'value_rank_history'")
conn.close()
