from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime, timedelta
import pandas as pd
import re
import time
import smtplib
from email.message import EmailMessage

start_time = time.time()



# ---------- Configuration ----------
CHROMEDRIVER_PATH    = "C:\\chromedriver\\chromedriver.exe"
DAYS_LOOKBACK        = 1
timestamp            = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_CSV           = f"ngx_director_dealings_{timestamp}.csv"

YAHOO_EMAIL          = "joyagbamuche@yahoo.co.uk"
YAHOO_APP_PASSWORD   = "xfjnminkphttlddq"  # generated from https://login.yahoo.com/account/security

# ---------- Setup Selenium ----------
options = Options()
options.add_argument("--headless")
service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)

def wait_for(selector, by=By.CSS_SELECTOR, timeout=20):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, selector))
    )

# ---------- Step 1: Load all listed companies with pagination ----------
print("🔍 Loading listed companies...")
driver.get("https://ngxgroup.com/exchange/trade/equities/listed-companies/")
time.sleep(5)

companies = []
page = 1

while True:
    print(f"\n📄 Scraping page {page}...")
    wait_for("tbody#ngx_companies_listed_securities tr")
    rows = driver.find_elements(By.CSS_SELECTOR, "tbody#ngx_companies_listed_securities tr")

    for row in rows:
        try:
            link = row.find_element(By.TAG_NAME, "a")
            name = link.text.strip()
            href = link.get_attribute("href")
            m = re.search(r"isin=(NG\w+)&", href)
            if m:
                companies.append({
                    "name": name,
                    "isin": m.group(1),
                    "profile_url": href
                })
        except:
            continue

    try:
        next_btn = driver.find_element(
            By.XPATH,
            '//a[contains(@class,"paginate_button next") and text()="Next"]'
        )
        if "disabled" in next_btn.get_attribute("class"):
            print("✅ Reached last page.")
            break
        driver.execute_script("arguments[0].click();", next_btn)
        page += 1
        time.sleep(4)
    except Exception as e:
        print(f"❌ Pagination failed: {e}")
        break

print(f"\n✅ Parsed {len(companies)} companies with ISINs.")

# ---------- Step 2: Scrape Director Dealings ----------
cutoff = datetime.now() - timedelta(days=DAYS_LOOKBACK)
results = []

for idx, comp in enumerate(companies, start=1):
    print(f"\n[{idx}/{len(companies)}] {comp['name']} ({comp['isin']})")
    driver.get(comp["profile_url"])
    time.sleep(2)

    try:
        tab = driver.find_element(By.LINK_TEXT, "DIRECTOR DEALINGS")
        tab.click()
        time.sleep(1)
    except:
        pass

    try:
        wait_for("tbody#ngx_dirDealings tr", timeout=5)
    except:
        print("   ⚠️ No Director Dealings section.")
        continue

    deal_rows = driver.find_elements(By.CSS_SELECTOR, "tbody#ngx_dirDealings tr")
    print(f"   📑 {len(deal_rows)} rows found.")

    for dr in deal_rows:
        try:
            td = dr.find_element(By.TAG_NAME, "td")
            html = td.get_attribute("innerHTML")
            if "Uploaded on" not in html:
                continue

            link_tag = td.find_element(By.TAG_NAME, "a")
            title = link_tag.text.strip()
            pdf_link = link_tag.get_attribute("href")

            dm = re.search(
                r"Uploaded on:\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s+\d{4})",
                html, re.IGNORECASE
            )
            if not dm:
                continue

            dstr = dm.group(1).replace("st", "").replace("nd", "")\
                             .replace("rd", "").replace("th", "").strip()
            doc_date = datetime.strptime(dstr, "%B %d %Y")

            if doc_date >= cutoff:
                results.append({
                    "Company": comp["name"],
                    "ISIN": comp["isin"],
                    "Title": title,
                    "Upload Date": doc_date.strftime("%Y-%m-%d"),
                    "PDF Link": pdf_link
                })
                print(f"     ✅ {title} ({doc_date.date()})")
        except Exception:
            continue

# ---------- Step 3: Save to CSV ----------
driver.quit()
df = pd.DataFrame(results)
df.to_csv(OUTPUT_CSV, index=False)
print(f"\n✅ Completed. {len(df)} documents saved to {OUTPUT_CSV}")

# ---------- Step 4: Email ----------
def send_email():
    if not results:
        print("📭 No new dealings — skipping email.")
        return

    msg = EmailMessage()
    msg["Subject"] = "NGX Director Dealings Report"
    msg["From"] = YAHOO_EMAIL
    msg["To"] = YAHOO_EMAIL
    msg.set_content("Attached is the latest NGX Director Dealings report.")

    with open(OUTPUT_CSV, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="octet-stream",
            filename=OUTPUT_CSV
        )

    try:
        with smtplib.SMTP_SSL("smtp.mail.yahoo.com", 465) as smtp:
            smtp.login(YAHOO_EMAIL, YAHOO_APP_PASSWORD)
            smtp.send_message(msg)
            print("📧 Email sent successfully!")
    except Exception as e:
        print(f"⚠️ Email failed: {e}")

send_email()

end_time = time.time()
print(f"\n⏱ Total time: {round((end_time - start_time) / 60, 2)} minutes")