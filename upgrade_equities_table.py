import sqlite3
import pandas as pd

DB_PATH = "data/ngx_equities.db"  # adjust if yours is in a different folder

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# === Step 1: Add missing columns (if they don't exist) ===
columns_to_add = {
    "volume_5_avg": "REAL",
    "volume_15_avg": "REAL",
    "price_5_high": "REAL"
}

for col, col_type in columns_to_add.items():
    try:
        cursor.execute(f"ALTER TABLE equities ADD COLUMN {col} {col_type}")
        print(f"✅ Added column: {col}")
    except sqlite3.OperationalError:
        print(f"ℹ️ Column {col} already exists. Skipping...")

conn.commit()

# === Step 2: Load data and calculate rolling metrics ===
df = pd.read_sql("SELECT * FROM equities ORDER BY name, date ASC", conn)
df["date"] = pd.to_datetime(df["date"])

# === Step 3: Calculate metrics grouped by stock name ===
df["volume_5_avg"] = df.groupby("name")["volume"].rolling(5, min_periods=1).mean().reset_index(0, drop=True)
df["volume_15_avg"] = df.groupby("name")["volume"].rolling(15, min_periods=1).mean().reset_index(0, drop=True)
df["price_5_high"] = df.groupby("name")["high"].rolling(5, min_periods=1).max().reset_index(0, drop=True)

# === Step 4: Overwrite enriched rows into DB ===
df.to_sql("equities", conn, if_exists="replace", index=False)
print("✅ Table updated with rolling metrics.")

conn.close()
