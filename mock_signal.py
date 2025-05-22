# mock_signal.py
import sqlite3
from datetime import datetime

conn = sqlite3.connect("data/ngx_equities.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS signals (
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

cursor.execute("""
INSERT INTO signals (name, date, signal, confidence_score, volume, trades, value, close, change, action, buy_range, explanation, limit_up_streak)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (
    "GTCO", datetime.today().strftime("%Y-%m-%d"),
    "🚀 Limit-Up Breakout", 95,
    10_000_000, 120, 75_000_000.0, 34.5, 1.8,
    "BUY CONFIRMED", "₦34.00 – ₦34.50", "Limit-Up Streak: 3 day(s), Moon Volume, Low Trades",
    3
))

conn.commit()
conn.close()
print("✅ Mock signal inserted.")
