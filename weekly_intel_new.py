# weekly_intel.py (enhanced breakout + stealth engine)

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

DB_PATH = "data/ngx_equities.db"

# Load recent 4 days of equities data
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values("date")

# Get all unique trading days (weekdays only)
all_days = df["date"].drop_duplicates().sort_values().tolist()

# Backtest from 30 trading days ago to today
lookback_days = 30
start_index = len(all_days) - lookback_days

for i in tqdm(range(start_index, len(all_days))):
    latest_date = all_days[i]
    
    # Ensure there are at least 20 prior days
    date_window = all_days[max(i - 29, 0): i + 1]  # full 30-day window
    if len(date_window) < 30:
        continue

    period_0_start = date_window[0]
    period_0_end   = date_window[14]
    period_1_start = date_window[15]
    period_1_end   = date_window[29]

    # Apply strict 10-day windows
    p0 = df[(df['date'] >= period_0_start) & (df['date'] <= period_0_end)]
    p1 = df[(df['date'] >= period_1_start) & (df['date'] <= period_1_end)]

    # Skip if no data
    if p1.empty or p0.empty:
        continue

 # Aggregations
    agg1 = p1.groupby("name").agg({
        "close": ["first", "last", "max"],
        "volume": "sum",
        "trades": "sum"
    }).reset_index()
    agg1.columns = ["name", "close_start_1", "close_end_1", "close_max_1", "volume_1", "trades_1"]

    agg0 = p0.groupby("name").agg({
        "volume": "sum",
        "trades": "sum"
    }).reset_index()
    agg0.columns = ["name", "volume_0", "trades_0"]

    avg_change_0 = p0.groupby("name")["change"].mean().reset_index().rename(columns={"change": "avg_change_0"})
    avg_change_1 = p1.groupby("name")["change"].mean().reset_index().rename(columns={"change": "avg_change_1"})
    
   

# Merge all
    intel_df = agg1.merge(agg0, on="name", how="left")
    intel_df = intel_df.merge(avg_change_0, on="name", how="left")
    intel_df = intel_df.merge(avg_change_1, on="name", how="left")

    # === New: Add today's (last row in p1) price + volume spike detection ===
    # Get today's row for each stock
    p1_today = p1.groupby("name").tail(1).copy()

    # Calculate today's % price change
    p1_today["price_spike_today"] = (p1_today["close"] - p1_today["open"]) / p1_today["open"] > 0.05

    # Calculate today's volume spike vs 10-day average volume in p1
    volume_avg = p1.groupby("name")["volume"].mean().reset_index()
    volume_avg.columns = ["name", "volume_avg_10"]
    p1_today = p1_today.merge(volume_avg, on="name", how="left")
    p1_today["volume_spike_today"] = p1_today["volume"] > 1.5 * p1_today["volume_avg_10"]

    # Select only what's needed
    p1_today_flags = p1_today[["name", "price_spike_today", "volume_spike_today"]]

    # Merge into intel_df
    intel_df = intel_df.merge(p1_today_flags, on="name", how="left")
    intel_df[["price_spike_today", "volume_spike_today"]] = intel_df[["price_spike_today", "volume_spike_today"]].fillna(False)

    # Optional: one final combined flag
     

# Flags
# === Base Flags (as before) ===
intel_df['trade_spike'] = intel_df['trades_1'] > 1.7 * intel_df['trades_0']
intel_df['volume_spike'] = intel_df['volume_1'] > 1.7 * intel_df['volume_0']
intel_df["price_up"] = intel_df["close_end_1"] > intel_df["close_start_1"]
intel_df["price_flip_up"] = (intel_df["avg_change_0"] < 0) & (intel_df["avg_change_1"] > 0)
intel_df["volume_slope"] = intel_df["volume_1"] > intel_df["volume_0"] * 1.3
intel_df["breakout_high"] = intel_df["close_end_1"] >= intel_df["close_max_1"]
intel_df["flat_high_volume"] = (
    (abs(intel_df["close_end_1"] - intel_df["close_start_1"]) <= 0.5) &
    (intel_df["volume_1"] >= intel_df["volume_0"] * 1.1)
)

# === Core Signals
intel_df['stealth_accum_candidate'] = (
    intel_df['volume_slope'] & (intel_df['avg_change_1'].abs() < 2.5)
)
intel_df['momentum_spike'] = (
    (intel_df['avg_change_1'] > 3.0) & intel_df['volume_spike']
)

# === Combo logic embedded directly as boolean score flags
intel_df['combo_flat'] = intel_df['flat_high_volume'] & intel_df['volume_slope'] & (~intel_df["breakout_high"])
intel_df['combo_reversal'] = intel_df['trade_spike'] & intel_df['price_flip_up'] & intel_df['volume_slope']
intel_df['combo_climb'] = intel_df['price_up'] & (~intel_df['volume_spike']) & (~intel_df["breakout_high"])
intel_df['combo_early_momentum'] = intel_df['volume_spike'] & intel_df['trade_spike'] & (~intel_df["breakout_high"])
intel_df["momentum_spike1"] = intel_df["price_spike_today"] & intel_df["volume_spike_today"] 

# === Final Score includes base and combo signals
intel_df['score'] = (
        intel_df[
        [
            'trade_spike', 'volume_spike', 'stealth_accum_candidate',
            "price_flip_up", 'momentum_spike', "volume_slope", "breakout_high","flat_high_volume",
            'combo_flat', 'combo_reversal', 'combo_climb', 'combo_early_momentum','momentum_spike1'
        ]
    ].sum(axis=1)
    + intel_df["momentum_spike"] * 1.5  # gives momentum spike extra weight
)

# === Tagging (unchanged logic)
def tag_score(score):
    if score >= 5:
        return "🚀 Institutional Zone - Strong buy"
    elif score == 4:
        return "🔥 HBreakout Brewing - Buy small"
    elif score == 3:
        return "⚠️ Warming Up -Watch for entry in the next 1–3 days if signal strengthens"
    elif score == 2:
        return "👀 Warming Up"
    else:
        return "—"

intel_df['trend_tag'] = intel_df['score'].apply(tag_score)
intel_df['date_generated'] = datetime.now().strftime("%Y-%m-%d")
intel_df.drop(columns=["price_flip_up"], inplace=True, errors="ignore")
intel_df.drop(columns=["volume_slope"], inplace=True, errors="ignore")
intel_df.drop(columns=["price_up"], inplace=True, errors="ignore")
intel_df.drop(columns=["close_start_0"], inplace=True, errors="ignore")
intel_df.drop(columns=["close_end_0"], inplace=True, errors="ignore")
intel_df.drop(columns=["breakout_high"], inplace=True, errors="ignore")
intel_df.drop(columns=["flat_high_volume"], inplace=True, errors="ignore")
intel_df.drop(columns=["combo_flat"], inplace=True, errors="ignore")
intel_df.drop(columns=["combo_reversal"], inplace=True, errors="ignore")
intel_df.drop(columns=["combo_early_momentum"], inplace=True, errors="ignore")
intel_df.drop(columns=["confirmed_breakout"], inplace=True, errors="ignore")
intel_df.drop(columns=["combo_climb"], inplace=True, errors="ignore")
intel_df.drop(columns=["valid_stealth"], inplace=True, errors="ignore")

intel_df.drop(columns=["momentum_spike1"], inplace=True, errors="ignore")
intel_df.drop(columns=["price_spike_today"], inplace=True, errors="ignore")
intel_df.drop(columns=["volume_spike_today"], inplace=True, errors="ignore")


# Save to DB
conn.execute("""
CREATE TABLE IF NOT EXISTS weekly_intel (
    name TEXT,
    trades_0 INTEGER,
    trades_1 INTEGER,
    trade_spike BOOLEAN,
    volume_0 INTEGER,
    volume_1 INTEGER,
    volume_spike BOOLEAN,
    avg_change_1 REAL,
    avg_change_0 REAL,
    close_start_1 REAL,
    close_end_1 REAL,
    close_max_1 REAL,
    stealth_accum_candidate BOOLEAN,
    momentum_spike BOOLEAN,
    volume_slope BOOLEAN,
    score INTEGER,
    trend_tag TEXT,
    date_generated TEXT
)
""")

conn.execute("DELETE FROM weekly_intel WHERE date_generated = ?", (intel_df['date_generated'][0],))
intel_df.to_sql("weekly_intel", conn, if_exists='append', index=False)
conn.close()

print(f"✅ Weekly Trade Intelligence saved with {len(intel_df)} records for {intel_df['date_generated'][0]}")
