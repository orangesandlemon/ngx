# news_scraper.py
import feedparser
import sqlite3
from datetime import datetime

DB_PATH = "data/us_equities.db"
TICKERS = ["AAPL", "TSLA", "AMZN", "META", "NVDA", "SPY", "QQQ", "AMD"]

def fetch_news(ticker):
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    return feedparser.parse(url).entries

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    headline TEXT,
    published_at TEXT,
    source TEXT
)
""")

for ticker in TICKERS:
    entries = fetch_news(ticker)
    for entry in entries:
        headline = entry.title
        published = datetime(*entry.published_parsed[:6]).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO news (name, headline, published_at, source)
            VALUES (?, ?, ?, ?)
        """, (ticker, headline, published, "Yahoo RSS"))

conn.commit()
conn.close()

print(" News scraping complete.")
