import sqlite3
import pandas as pd

# === CONFIG ===
DB_PATH = "data/ngx_equities.db"
WINDOW = 5  # number of rolling days

# === Connect to DB ===
conn = sqlite3.connect(DB_PATH)

# === Get last N days of value_rank_history ===
df = pd.read_sql_query(
    f"""
    SELECT * FROM value_rank_history
    WHERE date IN (
        SELECT DISTINCT date FROM value_rank_history
        ORDER BY date DESC
        LIMIT {WINDOW}
    )
""",
    conn,
)

df["date"] = pd.to_datetime(df["date"])
df.sort_values(by=["name", "date"], inplace=True)

# === Generate signals ===
signals = []

for name, group in df.groupby("name"):
    if len(group) < WINDOW:
        continue

    group = group.reset_index(drop=True)
    value_start = group.loc[0, "value"]
    value_end = group.loc[WINDOW - 1, "value"]
    rank_start = group.loc[0, "value_rank"]
    rank_end = group.loc[WINDOW - 1, "value_rank"]

    # Apply filters
    if value_end < 2 * value_start:
        continue
    if rank_end >= rank_start or rank_end >= 15:
        continue
    price_ok = all(
        (row["close"] >= row["open"]) and (row["close"] >= row["previous_close"])
        for _, row in group.iterrows()
    )
    if not price_ok:
        continue

    # Passed all checks
    signals.append(
        {
            "name": name,
            "date": group.loc[WINDOW - 1, "date"].strftime("%Y-%m-%d"),
            "rank_5_days_ago": rank_start,
            "current_rank": rank_end,
            "value_growth_pct": round((value_end - value_start) / value_start * 100, 1),
            "value_5_days_ago": value_start,
            "value_today": value_end,
            "signal": "VALUE_BUY_WATCH",
        }
    )

# === Store in SQLite Table: rank_signals
signals_df = pd.DataFrame(signals)

if not signals_df.empty:
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS rank_signals (
        name TEXT,
        date TEXT,
        rank_5_days_ago INTEGER,
        current_rank INTEGER,
        value_growth_pct REAL,
        value_5_days_ago REAL,
        value_today REAL,
        signal TEXT
    )
    """
    )

    signals_df.to_sql("rank_signals", conn, if_exists="append", index=False)
    print(f"[✓] {len(signals_df)} signals saved to 'rank_signals'")

else:
    print("[ℹ] No qualifying signals today.")

conn.close()
