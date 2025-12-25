# scraper.py
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime

# Skip if it's Saturday (5) or Sunday (6)
if datetime.today().weekday() >= 5:
    print("🛑 Market closed (weekend). Exiting.")
    exit()

# === CONFIGURATION ===
CHROMEDRIVER_PATH = "C:\\chromedriver\\chromedriver.exe"
DB_PATH = "data/ngx_equities.db"
START_URL = "https://ngxgroup.com/exchange/data/equities-price-list/"

# === SETUP DRIVER ===
options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)

# === UTILS ===
def clean(text):
    return text.strip().replace(",", "").replace("₦", "").replace("--", "").replace("\u00a0", "")

def safe_float(text):
    try:
        return float(clean(text))
    except:
        return 0.0

def safe_int(text):
    try:
        return int(clean(text))
    except:
        return 0

# === SCRAPE A SINGLE PAGE ===
def scrape_current_page():
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable"))
        )
        time.sleep(1)  # let JS finish

        rows = driver.find_elements(By.CSS_SELECTOR, "table.dataTable tbody tr")
        print(f"   ⏳ Found {len(rows)} rows")

        data = []
        for i, row in enumerate(rows):
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 11:
                continue

            try:
                if i == 0:
                    print("🧪 Sample row data:")
                    for idx, col in enumerate(cols):
                        print(f"  Col {idx}: {col.text}")

                record = {
                    "name": cols[0].text.strip(),
                    "previous_close": safe_float(cols[1].text),
                    "open": safe_float(cols[2].text),
                    "high": safe_float(cols[3].text),
                    "low": safe_float(cols[4].text),
                    "close": safe_float(cols[5].text),
                    "change": safe_float(cols[6].text.replace("%", "")),
                    "trades": safe_int(cols[7].text),
                    "volume": safe_int(cols[8].text),
                    "value": safe_float(cols[9].text),
                    "date": datetime.strptime(cols[10].text.strip(), "%d %b %y").strftime("%Y-%m-%d")
                }
                data.append(record)

            except Exception as e:
                print(f"   ⚠️ Skipping row due to error: {e}")

        return data  # ✅ <-- this was missing

    except Exception as e:
        print(f"❌ Error scraping current page: {e}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        driver.save_screenshot(f"screenshots/fail_{timestamp}.png")
        return []


    
# === HANDLE PAGINATION ===
def scrape_all_pages():
    print("🚀 Starting NGX equities scrape...")
    driver.get(START_URL)
    time.sleep(3)

    all_data = []
    page = 1

    while True:
        print(f"\n📄 Scraping page {page}")
        page_data = scrape_current_page()
        if not page_data:
            print("⚠️ No data found on this page.")
            break
        all_data.extend(page_data)

        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "a.paginate_button.next")
            if "disabled" in next_btn.get_attribute("class"):
                print("✅ Reached last page.")
                break
            driver.execute_script("arguments[0].click();", next_btn)
            page += 1
            time.sleep(2)
        except:
            print("✅ No more pages or pagination failed.")
            break

    return pd.DataFrame(all_data)

# === STORE TO SQLITE ===
def store_to_db(df):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")

    # ✅ Load tickers that have a marketcap
    marketcap_df = pd.read_sql(
        "SELECT name, marketcap FROM equities WHERE marketcap IS NOT NULL AND marketcap != '' GROUP BY name", conn
    )

    # ✅ Merge to keep only those with a known marketcap
    df_filtered = df.merge(marketcap_df, on='name', how='inner')  # only stocks with marketcap

    print(f"🔍 Filtered {len(df_filtered)} stocks with marketcap out of {len(df)} total scraped.")

    # 🧹 Delete today's records for only these names
    for name in df_filtered['name'].unique():
        conn.execute("DELETE FROM equities WHERE name = ? AND date = ?", (name, today))

    # ✅ Insert filtered rows
    df_filtered.to_sql("equities", conn, if_exists="append", index=False, method="multi")
    conn.close()
    print(f"✅ Stored {len(df_filtered)} records into database (marketcap required).")


# === MAIN ===
if __name__ == "__main__":
    os.makedirs("screenshots", exist_ok=True)
    df = scrape_all_pages()
    expected_cols = ['name', 'previous_close', 'open', 'high', 'low', 'close', 'change', 'trades', 'volume', 'value', 'date']
    if not all(col in df.columns for col in expected_cols):
        print("❌ Dataframe missing expected columns!")
        print("📋 Columns found:", df.columns.tolist())
        exit()

    driver.quit()

    if not df.empty:
        store_to_db(df)
        print(df.head())
    else:
        print("⚠️ No data to store.")
