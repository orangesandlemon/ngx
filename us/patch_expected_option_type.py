# patch_expected_option_type.py
import sqlite3
import pandas as pd

DB_PATH = "data/us_equities.db"
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM signals_us", conn)

# === Simple inference logic
def infer_option_type(row):
    explanation = row["explanation"].lower()
    if "price up" in explanation:
        return "Call"
    elif "price down" in explanation:
        return "Put"
    return None

# === Apply logic
df["expected_option_type"] = df.apply(infer_option_type, axis=1)

# === Write back to DB
cursor = conn.cursor()
for _, row in df.iterrows():
    cursor.execute("""
        UPDATE signals_us
        SET expected_option_type = ?
        WHERE name = ? AND date = ?
    """, (row["expected_option_type"], row["name"], row["date"]))

conn.commit()
conn.close()
print("✅ expected_option_type updated for all rows.")
