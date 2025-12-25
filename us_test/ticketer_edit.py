# Step 1: Read failed tickers from file
with open("failed_tickers.txt", "r") as f:
    failed = set(line.strip() for line in f if line.strip())

# Step 2: Import tickers
from us_scraper1 import TICKERS

# Step 3: Filter
filtered_tickers = [t for t in TICKERS if t not in failed]

# Step 4: Save as comma-separated quoted tickers
with open("filtered_tickers.txt", "w") as f_out:
    f_out.write(",".join(f'"{t}"' for t in filtered_tickers))

print("✅ Saved comma-separated quoted tickers to filtered_tickers.txt")
