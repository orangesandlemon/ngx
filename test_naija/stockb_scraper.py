from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import sqlite3
import time
from datetime import date

DB_PATH = "data/ngx_equities.db"


def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    return webdriver.Chrome(options=chrome_options)


def scrape_stockbubbles():
    url = "https://stockbubbles.com.ng/"
    driver = get_driver()
    driver.get(url)
    time.sleep(5)  # Wait for JS content to load

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    rows = soup.select("tbody tr")
    results = []

    for row in rows:
        try:
            name = row.select_one("img")["alt"].strip()
            tds = row.select("td")

            price = float(tds[2].get_text(strip=True).replace("₦", "").replace(",", ""))
            hour = float(tds[7].get_text(strip=True).replace("%", "").replace(",", ""))

            results.append((name, price, hour))
        except Exception as e:
            print("⚠️ Skipped row due to error:", e)
            continue

    return results


def update_db(data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # today = date.today().isoformat()  # '2025-08-07' format

    updated = 0
    for name, close, change_pct in data:
        try:
            cursor.execute(
                """
                UPDATE equities 
                SET close = ?, change_pct = ?
                WHERE name = ? 
            """,
                (
                    close,
                    change_pct,
                    name,
                ),  # today
            )

            if cursor.rowcount > 0:
                updated += 1
        except Exception as e:
            print(f"❌ Failed for {name}: {e}")

    conn.commit()
    conn.close()
    # print(f"✅ Done. {updated} rows updated for {today}.")


if __name__ == "__main__":
    print("🔍 Scraping...")
    scraped = scrape_stockbubbles()
    print(f"📦 Got {len(scraped)} items.")
    update_db(scraped)
