from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
import pandas as pd
import re
import time
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta

start_time = time.time()

timestamp = datetime.now().strftime("%Y%m%d_%H%M")


# ---------- Configuration ----------
CHROMEDRIVER_PATH = "C:\\chromedriver\\chromedriver.exe"
OUTPUT_CSV = OUTPUT_CSV = f"ngx_financial_statements_{timestamp}.csv"
YAHOO_EMAIL = "joyagbamuche@yahoo.co.uk"
YAHOO_APP_PASSWORD = "xfjnminkphttlddq"  # Keep safe!

# ---------- Setup Selenium ----------
options = Options()
options.add_argument("--headless")
service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)


def wait_for(selector, by=By.CSS_SELECTOR, timeout=20):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, selector))
    )


# ---------- Load Companies ----------
print("🔍 Loading listed companies...")
driver.get("https://ngxgroup.com/exchange/trade/equities/listed-companies/")
time.sleep(5)

companies = []
page = 1

while True:
    print(f"\n📄 Scraping page {page}...")
    wait_for("tbody#ngx_companies_listed_securities tr")
    rows = driver.find_elements(
        By.CSS_SELECTOR, "tbody#ngx_companies_listed_securities tr"
    )

    for row in rows:
        try:
            link = row.find_element(By.TAG_NAME, "a")
            name = link.text.strip()
            href = link.get_attribute("href")
            m = re.search(r"isin=(NG\w+)&", href)
            if m:
                companies.append(
                    {"name": name, "isin": m.group(1), "profile_url": href}
                )
        except:
            continue

    try:
        next_btn = driver.find_element(
            By.XPATH, '//a[contains(@class,"paginate_button next") and text()="Next"]'
        )
        if "disabled" in next_btn.get_attribute("class"):
            break
        driver.execute_script("arguments[0].click();", next_btn)
        page += 1
        time.sleep(3.5)
    except Exception as e:
        print(f"❌ Pagination stopped: {e}")
        break

print(f"\n✅ Parsed {len(companies)} companies.")

# ---------- Scan Financial Statements ----------
results = []

for idx, comp in enumerate(companies, start=1):
    print(f"\n[{idx}/{len(companies)}] {comp['name']} ({comp['isin']})")
    driver.get(comp["profile_url"])
    time.sleep(3)

    # ✅ Cookie bar blocker fix
    try:
        cookie_bar = driver.find_element(By.ID, "cookie-law-info-bar")
        if cookie_bar.is_displayed():
            accept_btn = driver.find_element(By.ID, "cookie_action_close_header")
            accept_btn.click()
            print("🍪 Cookie bar dismissed.")
            time.sleep(1)
    except:
        pass

    try:
        labels = driver.find_elements(By.TAG_NAME, "label")
        found = False
        for label in labels:
            if "financial" in label.text.lower():
                label.click()
                found = True
                print(f"✅ Clicked financials tab for {comp['name']}")
                time.sleep(3)  # allow tab to load
                break
        if not found:
            print(f"❌ Financials tab not found for {comp['name']}")
            continue

        wait_for("tbody#ngx_finStatement tr", timeout=5)

    except Exception as e:
        print(f"⚠️ Error finding tab for {comp['name']}: {e}")
        continue

    rows = driver.find_elements(By.CSS_SELECTOR, "tbody#ngx_finStatement tr")
    time.sleep(1.2)  # small buffer after rows load
    if not rows:
        print(f"📭 No financials found for {comp['name']}")
        continue

    for r in rows:
        try:
            td = r.find_element(By.TAG_NAME, "td")
            html = td.get_attribute("innerHTML")
            if "Uploaded on" not in html:
                continue

            link_tag = td.find_element(By.TAG_NAME, "a")
            title = link_tag.text.strip()
            pdf_link = link_tag.get_attribute("href")

            dm = re.search(
                r"Uploaded on:\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s+\d{4})", html
            )
            if not dm:
                continue

            dstr = (
                dm.group(1)
                .replace("st", "")
                .replace("nd", "")
                .replace("rd", "")
                .replace("th", "")
                .strip()
            )

            # Handle known typos
            for wrong, correct in {
                "Augu": "August",
                "Novemberber": "November",
                "Octoberber": "October",
            }.items():
                dstr = dstr.replace(wrong, correct)

            doc_date = datetime.strptime(dstr, "%B %d %Y")
            doc_date_str = doc_date.strftime("%Y-%m-%d")
            # today_str = datetime.now().strftime("%Y-%m-%d")
            today = datetime.now().date().strftime("%Y-%m-%d")
            # yesterday = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")

            # if doc_date.date() in [
            if doc_date_str in [today]:  # doc_date.strftime("%Y-%m-%d") == today_str:
                print(f"📢 NEW UPLOAD: {title} | Date: {doc_date_str}")
                results.append(
                    {
                        "Company": comp["name"],
                        "ISIN": comp["isin"],
                        "Title": title,
                        "Upload Date": doc_date.strftime("%Y-%m-%d"),
                        "PDF Link": pdf_link,
                    }
                )
                print(f"   ✅ New upload: {title}")
        except Exception as e:
            print(f"   ⚠️ Error: {e}")
            continue

# ---------- Save ----------
driver.quit()
df = pd.DataFrame(results)
df.to_csv(OUTPUT_CSV, index=False)
print(f"\n✅ Done. {len(df)} new financial statements saved to {OUTPUT_CSV}")


# ---------- Email ----------
def send_email():
    if not results:
        print("📭 No new financials today — no email sent.")
        return

    msg = EmailMessage()
    msg["Subject"] = "📊 New NGX Financial Statement(s) Uploaded"
    msg["From"] = YAHOO_EMAIL
    msg["To"] = YAHOO_EMAIL
    msg.set_content("Attached are the newly uploaded NGX financial statements.")

    with open(OUTPUT_CSV, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="octet-stream",
            filename=OUTPUT_CSV,
        )

    try:
        with smtplib.SMTP_SSL("smtp.mail.yahoo.com", 465) as smtp:
            smtp.login(YAHOO_EMAIL, YAHOO_APP_PASSWORD)
            smtp.send_message(msg)
            print("📧 Email sent!")
    except Exception as e:
        print(f"⚠️ Email failed: {e}")


send_email()

end_time = time.time()
print(f"\n⏱ Total time: {round((end_time - start_time) / 60, 2)} minutes")
