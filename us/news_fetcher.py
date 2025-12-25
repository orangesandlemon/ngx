# news_fetcher.py (Yahoo RSS version with Trump sentiment logic)
import sqlite3
import feedparser
from datetime import datetime
import time
import os

DB_PATH = "data/us_equities.db"

company_name_map = {
    "SPY": "S&P 500 ETF",
    "QQQ": "Nasdaq-100 ETF",
    "IWM": "Russell 2000 ETF",
    "DIA": "Dow Jones ETF",
    "VIX": "Volatility Index",

    "TSLA": "Tesla",
    "AAPL": "Apple",
    "NVDA": "NVIDIA",
    "AMD": "Advanced Micro Devices",
    "AMZN": "Amazon",
    "MSFT": "Microsoft",
    "META": "Meta Platforms",
    "GOOGL": "Alphabet",

    "NFLX": "Netflix",
    "BA": "Boeing",
    "COIN": "Coinbase",
    "BABA": "Alibaba",
    "INTC": "Intel",
    "PLTR": "Palantir",
    "XOM": "ExxonMobil",
    "CVX": "Chevron",
    "JPM": "JPMorgan Chase",
    "WMT": "Walmart",
    "COST": "Costco"
}


tickers = [
    # 🔹 ETFs (Highly Liquid)
    "SPY",    # S&P 500 ETF
    "QQQ",    # Nasdaq-100 ETF
    "IWM",    # Russell 2000 ETF
    "DIA",    # Dow Jones ETF
    "VIX",    # Volatility Index

    # 🔹 Mega Cap Stocks
    "TSLA",   # Tesla
    "AAPL",   # Apple
    "NVDA",   # NVIDIA
    "AMD",    # AMD
    "AMZN",   # Amazon
    "MSFT",   # Microsoft
    "META",   # Meta Platforms
    "GOOGL",  # Alphabet (Google)

    # 🔹 Liquid Mid-Large Caps
    "NFLX",   # Netflix
    "BA",     # Boeing
    "COIN",   # Coinbase
    "BABA",   # Alibaba
    "INTC",   # Intel
    "PLTR",   # Palantir
    "XOM",    # ExxonMobil
    "CVX",    # Chevron
    "JPM",    # JPMorgan Chase
    "WMT",    # Walmart
    "COST"    # Costco
]


bullish_keywords = [
    "beats", "surge", "growth", "strong", "record", "supports", "cut taxes",
    "optimism", "rally", "soars", "outperform", "positive", "bullish", "boost stock"
]
bearish_keywords = [
    "miss", "fall", "drop", "weak", "loss", "threatens", "tariff", "ban", "bans",
    "strike", "misses", "lawsuit", "cut", "slumps", "downgrade", "decline", "negative", "bearish", "slow growth", "growth slow"
]

trump_positive = [
    "supports", "boosts", "cut taxes", "deregulate", "backs", "praises", "signs deal",
    "lowers taxes", "pro business", "stimulus", "economic growth"
]

trump_negative = [
    "threatens", "tariff", "ban", "attacks", "criticizes", "escalates", "restricts",
    "sanctions", "calls out", "destabilizes", "trade war"
]

def score_headline(headline):
    h_lower = headline.lower()
    score = 0
    reasons = []

    if any(word in h_lower for word in bullish_keywords):
        score += 10
        reasons.append("bullish keyword")
    if any(word in h_lower for word in bearish_keywords):
        score -= 10
        reasons.append("bearish keyword")
    if "trump" in h_lower:
        if any(w in h_lower for w in trump_positive):
            score += 10
            reasons.append("Trump boost")
        if any(w in h_lower for w in trump_negative):
            score -= 10
            reasons.append("Trump threat")

    return score, ", ".join(reasons) or "neutral"

def fetch_news(ticker):
    feed_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    return feedparser.parse(feed_url).entries

def process_news():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_signals (
            name TEXT,
            date TEXT,
            headline TEXT,
            news_score INTEGER,
            news_reason TEXT,
            timestamp TEXT,
            PRIMARY KEY (name, date, headline)
        )
    """)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for ticker in tickers:
        entries = fetch_news(ticker)
        print(f"{ticker}: {len(entries)} headlines fetched")

        for entry in entries:
            headline = entry.get("title", "")
            published = entry.get("published", "")
            try:
                published_date = datetime.strptime(published, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
            except:
                published_date = datetime.now().strftime("%Y-%m-%d")

            if headline:
                score, reason = score_headline(headline)
                if score != 0:
                    print(f" {ticker}: {headline}\n   → Score: {score} | Reason: {reason}")
                    cursor.execute("""
                        INSERT OR IGNORE INTO news_signals
                        (name, date, headline, news_score, news_reason, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (ticker, published_date, headline, score, reason, now))
                    time.sleep(0.2)

    conn.commit()
    conn.close()
    print(" News processing complete.")

if __name__ == "__main__":
    process_news()
