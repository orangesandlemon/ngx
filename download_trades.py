import os
import time
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

# === Load credentials ===
load_dotenv()
USERNAME = os.getenv("ATLAS_USERNAME")
PASSWORD = os.getenv("ATLAS_PASSWORD")

if not USERNAME or not PASSWORD:
    raise Exception("🛑 ATLAS_USERNAME or ATLAS_PASSWORD not set in .env")

# === Setup Chrome driver ===
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Uncomment to run invisibly
chrome_options.add_argument("--incognito")  # Avoid cached sessions
driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 15)

try:
    # === 1. Visit login page ===
    driver.get("https://app.atlassportfolios.com/customers/")
    time.sleep(2)

    # === 2. Enter login credentials ===
    username = driver.find_element(By.ID, "un")
    password = driver.find_element(By.ID, "pw")
    login_button = driver.find_element(By.ID, "btn")

    username.send_keys(USERNAME)
    password.send_keys(PASSWORD)

    # Wait for login button to be enabled
    while login_button.get_attribute("disabled"):
        time.sleep(0.5)

    login_button.click()

    # === 3. Wait for the hamburger menu and click it ===
    try:
        menu_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "div.dijitToggleButton"))
        )
        menu_btn.click()

        # Wait for sidebar to expand
        WebDriverWait(driver, 10).until(
            lambda d: d.find_element(By.ID, "__appSideNav")
            .get_attribute("style")
            .find("width: 0px")
            == -1
        )
        print("✅ Sidebar is open.")
    except Exception as e:
        print("❌ Failed to open hamburger menu:", e)

    # === 4. Click "Trades" in sidebar ===
    try:
        trades_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "sidenav_item_market_trades"))
        )
        trades_btn.click()
        print("✅ 'Trades' clicked.")
        time.sleep(2)
    except Exception as e:
        print("❌ Could not click 'Trades':", e)

    # === 5. Click refresh icon to load trade data ===
    try:
        refresh_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "div.appSideNavMenuIcon.applicationIcon.md-light.assessmentIcon",
                )
            )
        )
        refresh_btn.click()
        print("🔄 Refresh clicked to load trade data.")
        time.sleep(2)
    except Exception as e:
        print("⚠️ Could not click refresh button:", e)

    # === 6. Wait for loading overlay to disappear ===
    WebDriverWait(driver, 15).until(
        lambda d: d.find_element(By.ID, "loadingOverlay").value_of_css_property(
            "display"
        )
        == "none"
    )
    print("✅ Loading overlay cleared.")

    # === 7. Wait until rows appear ===
    WebDriverWait(driver, 15).until(
        lambda d: len(d.find_elements(By.CSS_SELECTOR, ".dojoxGridRow")) > 0
    )
    rows = driver.find_elements(By.CSS_SELECTOR, ".dojoxGridRow")
    print(f"📊 Loaded {len(rows)} trades.")

    # === 8. Scrape trade data ===
    all_data = []
    for row in rows:
        cells = row.find_elements(By.CLASS_NAME, "dojoxGridCell")
        trade_row = [cell.text.strip() for cell in cells]
        if trade_row:
            all_data.append(trade_row)

    # === 9. Save to CSV if data found ===
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"atlas_trades_{today}.csv"

    if all_data:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Symbol", "Price", "Qty", "Value", "Change", "Trade Time"])
            writer.writerows(all_data)
        print(f"✅ Trades saved to {filename}")
    else:
        print("⚠️ No trades found — CSV not saved.")

except Exception as e:
    print("❌ An error occurred:", e)

finally:
    try:
        # === 10. Open user dropdown menu ===
        user_icon = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "userMenuButton"))
        )
        user_icon.click()
        print("👤 User menu opened.")
        time.sleep(1)

        # === 11. Click logout from dropdown ===
        logout_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "dijit_MenuItem_1"))
        )
        logout_btn.click()
        print("🔓 Logged out via dropdown.")
        time.sleep(2)

    except Exception as e:
        print("⚠️ Logout failed:", e)

    driver.quit()
