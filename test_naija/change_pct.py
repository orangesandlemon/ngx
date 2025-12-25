import sqlite3
import pandas as pd

DB_PATH = "data/ngx_equities.db"

# Connect to DB
conn = sqlite3.connect(DB_PATH)

# Step 1: Load all rows where change_pct is NULL or 0 (not already filled)
query = """
SELECT rowid, name, date, previous_close, close, change_pct
FROM equities
WHERE change_pct IS NULL OR change_pct = 0
"""
df = pd.read_sql_query(query, conn)

if df.empty:
    print("✅ All rows already have change_pct. Nothing to update.")
    conn.close()
else:
    print(f"🔧 Found {len(df)} rows to update...")

    # Step 2: Calculate change_pct safely
    df["change_pct"] = (
        (df["close"] - df["previous_close"]) / df["previous_close"]
    ) * 100
    df["change_pct"] = df["change_pct"].round(2)

    # Step 3: Update DB using rowid
    for _, row in df.iterrows():
        conn.execute(
            """
            UPDATE equities
            SET change_pct = ?
            WHERE rowid = ?
        """,
            (row["change_pct"], row["rowid"]),
        )

    conn.commit()
    conn.close()
    print(f"✅ Updated {len(df)} rows with calculated change_pct.")
