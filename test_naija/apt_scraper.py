import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import sqlite3

DB_PATH = "data/ngx_equities.db"
TABLE_NAME = "equities"

# === Step 1: Get last 30 weekdays
today = datetime.today().date()
trading_dates = []
while len(trading_dates) < 90:
    if today.weekday() < 5:
        trading_dates.append(today)
    today -= timedelta(days=1)
trading_dates.reverse()

# === Step 2: Connect to DB
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        name TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        trades INTEGER,
        volume INTEGER,
        value REAL,
        PRIMARY KEY (name, date)
    )
""")

# === Step 3: Clean helpers
def clean_number(text):
    if text is None:
        return None
    text = text.strip().replace(",", "")
    try:
        return int(text)
    except:
        try:
            return float(text)
        except:
            return None

# === Step 4: Scrape each date
for date_obj in trading_dates:
    date_str = date_obj.strftime("%Y-%m-%d")
    url = f"https://www.aptsecurities.com/nse-daily-price.php?date={date_str}"
    try:
        resp = requests.get(url, timeout=10)
    except Exception as e:
        print(f"❌ Failed to fetch {date_str}: {e}")
        continue
    if resp.status_code != 200:
        print(f"⚠️ {date_str} - HTTP {resp.status_code}")
        continue

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.find_all("tr")

    count = 0
    for row in rows:
        name_cell = row.find("th")
        cells = row.find_all("td")
        if not name_cell or len(cells) < 8:
            continue

        name = name_cell.get_text(strip=True)
        open_val   = clean_number(cells[0].get_text(strip=True))
        close_val  = clean_number(cells[1].get_text(strip=True))
        high_val   = clean_number(cells[2].get_text(strip=True))
        low_val    = clean_number(cells[3].get_text(strip=True))
        trades_val = clean_number(cells[5].get_text(strip=True))
        volume_val = clean_number(cells[6].get_text(strip=True))
        value_val  = clean_number(cells[7].get_text(strip=True))

        cursor.execute(f"""
            INSERT OR IGNORE INTO {TABLE_NAME} 
            (name, date, open, high, low, close, trades, volume, value) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, date_str, open_val, high_val, low_val, close_val, trades_val, volume_val, value_val))
        count += 1

    conn.commit()
    print(f"✅ {date_str}: {count} records")

conn.close()
print("🎯 Done!")
