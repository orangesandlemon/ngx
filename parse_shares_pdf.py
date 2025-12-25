import pdfplumber
import csv
import re

pdf_path = "SHARES OUTSTANDING FOR 31-10-2025.pdf"
output_csv = "stocks_with_main_and_subsector.csv"

results = []
current_main_sector = None
current_sub_sector = None

def is_main_sector(line):
    return (
        line.strip().isupper()
        and "-" not in line
        and "VALUE" not in line
        and len(line.strip()) > 2 and len(line.strip()) < 50
    )

def is_sub_sector(line):
    return bool(re.match(r"^[A-Z/&\.\s]+\- [A-Za-z\s/&\.\(\)-]+$", line.strip()))

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        lines = page.extract_text().split("\n")

        for line in lines:
            line = line.strip()

            # Main sector
            if is_main_sector(line):
                current_main_sector = line
                continue

            # Sub-sector
            if is_sub_sector(line):
                current_sub_sector = line
                continue

            # Possible stock data
            parts = line.split()
            if len(parts) >= 5 and re.match(r"^[A-Z0-9]{2,10}$", parts[0]):
                try:
                    symbol = parts[0]
                    name = " ".join(parts[1:-3])
                    price = float(parts[-3].replace(",", ""))
                    shares_out = int(parts[-2].replace(",", ""))
                    market_cap = float(parts[-1].replace(",", ""))
                except:
                    continue

                results.append({
                    "symbol": symbol,
                    "name": name,
                    "price": price,
                    "shares_outstanding": shares_out,
                    "market_cap": market_cap,
                    "main_sector": current_main_sector,
                    "sub_sector": current_sub_sector
                })

# === Save to CSV ===
if results:
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ Extracted {len(results)} stocks → {output_csv}")
else:
    print("⚠️ No stock entries extracted. Check line patterns or try printing sample lines.")
