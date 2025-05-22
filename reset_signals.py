# reset_signals.py
import sqlite3

conn = sqlite3.connect("data/ngx_equities.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS signals")

cursor.execute("""
CREATE TABLE signals (
    name TEXT,
    date TEXT,
    signal TEXT,
    confidence_score INTEGER,
    volume INTEGER,
    trades INTEGER,
    value REAL,
    close REAL,
    change REAL,
    action TEXT,
    buy_range TEXT,
    explanation TEXT,
    limit_up_streak INTEGER
)
""")

conn.commit()
conn.close()
print("✅ signals table dropped and recreated with full schema.")
