# weekly_intel.py (enhanced breakout + stealth engine)

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "data/ngx_equities.db"

# Load recent 4 days of equities data
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities WHERE date >= DATE('now', '-4 day')", conn)
df['date'] = pd.to_datetime(df['date'])

# Define windows
latest_date = df['date'].max()
period_1_start = latest_date - timedelta(days=1)  # last 2 days = recent
period_0_start = latest_date - timedelta(days=3)  # prior 2 days = baseline

# Split periods
p1 = df[df['date'] >= period_1_start]
p0 = df[(df['date'] >= period_0_start) & (df['date'] < period_1_start)]

# Aggregate per stock
p1_agg = p1.groupby('name').agg(
    trades_1=('trades', 'sum'),
    volume_1=('volume', 'sum'),
    avg_change_1=('change', 'mean')
).reset_index()

p0_agg = p0.groupby('name').agg(
    trades_0=('trades', 'sum'),
    volume_0=('volume', 'sum')
).reset_index()

# Merge and fill NaNs
intel_df = pd.merge(p1_agg, p0_agg, on="name", how="outer").fillna(0)

# Flags
intel_df['trade_spike'] = intel_df['trades_1'] > 1.5 * intel_df['trades_0']
intel_df['volume_spike'] = intel_df['volume_1'] > 1.5 * intel_df['volume_0']
intel_df['stealth_accum_candidate'] = (intel_df['volume_spike']) & (intel_df['avg_change_1'].abs() < 3.0)
intel_df['momentum_spike'] = (intel_df['avg_change_1'] > 3.0) & (intel_df['volume_spike'])

# Score = sum of all boolean flags
intel_df['score'] = intel_df[['trade_spike', 'volume_spike', 'stealth_accum_candidate', 'momentum_spike']].sum(axis=1)

# Tag
def tag_score(score):
    if score == 4:
        return "🔥 Hot & Active"
    elif score == 3:
        return "⚠️ Breakout Brewing"
    elif score == 2:
        return "👀 Warming Up"
    else:
        return "—"

intel_df['trend_tag'] = intel_df['score'].apply(tag_score)
intel_df['date_generated'] = datetime.now().strftime("%Y-%m-%d")

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
    stealth_accum_candidate BOOLEAN,
    momentum_spike BOOLEAN,
    score INTEGER,
    trend_tag TEXT,
    date_generated TEXT
)
""")

if not intel_df.empty:
    conn.execute("DELETE FROM weekly_intel WHERE date_generated = ?", (intel_df['date_generated'].iloc[0],))
else:
    print("⚠️ No records generated — nothing to delete or save.")

intel_df.to_sql("weekly_intel", conn, if_exists='append', index=False)
conn.close()

print(f"✅ Weekly Trade Intelligence saved with {len(intel_df)} records for {intel_df['date_generated'][0]}")
