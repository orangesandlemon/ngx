import sqlite3
import pandas as pd

# === CONFIG ===
DB_PATH = "data/ngx_equities.db"
NUM_DAYS = 15  # backfill 15 days

# === Connect to DB ===
conn = sqlite3.connect(DB_PATH)

# === Get Last N Trading Dates ===
date_query = f"""
SELECT DISTINCT date FROM equities
ORDER BY date DESC
LIMIT {NUM_DAYS}
"""
dates = pd.read_sql_query(date_query, conn)["date"].sort_values().tolist()

# === Create Output Table if Not Exists ===
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

# === Backfill Loop ===
for current_date in dates:
    # Pull today's data
    df_today = pd.read_sql_query(
        f"""
        SELECT name, date, value, open, close
        FROM equities
        WHERE date = '{current_date}'
    """,
        conn,
    )

    # Skip zero value
    df_today = df_today[df_today["value"] > 0].copy()

    # Get previous close
    df_prev = pd.read_sql_query(
        f"""
        SELECT name, close AS previous_close
        FROM equities
        WHERE date = (
            SELECT MAX(date) FROM equities
            WHERE date < '{current_date}'
        )
    """,
        conn,
    )

    # Merge & rank
    df_today = df_today.merge(df_prev, on="name", how="left")
    df_today["value_rank"] = (
        df_today["value"].rank(method="min", ascending=False).astype(int)
    )

    # Save to history table
    df_today.to_sql("value_rank_history", conn, if_exists="append", index=False)
    print(f"[✓] Stored value ranks for {current_date} (rows: {len(df_today)})")

conn.close()
