# === smart_money_scanner.py ===
# This script analyzes NGX (or other) market data for signs of smart money activity
# such as accumulation, stealth buying, distribution, and more.

import pandas as pd
import numpy as np

# === Step 1: Load and Normalize Data ===
def load_and_prepare_data(csv_path):
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.dropna(subset=["close", "volume"])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['name', 'date'])
    return df

# === Step 2: Segment Market into Price Tiers ===
def classify_tier(price):
    if price < 10:
        return 'Penny'
    elif price < 100:
        return 'Mid'
    else:
        return 'High'

def add_price_tiers(df):
    df['price_tier'] = df['close'].apply(classify_tier)
    return df

# === Step 3: Detect Smart Money Footprints ===
def analyze_behavior(df):
    signals = []
    for name, group in df.groupby("name"):
        group = group.reset_index(drop=True)
        if len(group) < 15:
            continue

        group['vol_avg'] = group['volume'].rolling(5).mean()
        group['chg_pct'] = group['close'].pct_change()
        group['vol_chg'] = group['volume'].pct_change()
        group['vol_ratio'] = group['chg_pct'] / (group['vol_chg'] + 1e-6)
        group['price_range'] = group['high'] - group['low']

        recent = group.iloc[-1]

        # Pattern detection
        if recent['chg_pct'] < 0 and recent['volume'] > group['vol_avg'].mean():
            signal = '🧠 Absorption Zone (price down, vol up)'
        elif recent['vol_ratio'] > 0 and recent['chg_pct'] < 0.01:
            signal = '📦 Quiet Accumulation'
        elif (recent['volume'] > group['vol_avg'].mean()) and abs(recent['chg_pct']) < 0.005:
            signal = '🕵️ Stealth Buying (sideways + vol)'
        elif recent['chg_pct'] > 0.05 and recent['vol_chg'] < -0.2:
            signal = '🚩 Potential Distribution'
        elif recent['chg_pct'] < -0.05 and recent['vol_chg'] < -0.2:
            signal = '💀 Low-volume Dump'
        else:
            continue

        signals.append({
            'name': name,
            'date': recent['date'],
            'tier': recent['price_tier'],
            'close': recent['close'],
            'volume': recent['volume'],
            'signal': signal
        })

    return pd.DataFrame(signals)

# === Step 4: Rank Signals ===
def rank_signals(signal_df):
    priority = {
        '🕵️ Stealth Buying (sideways + vol)': 3,
        '📦 Quiet Accumulation': 3,
        '🧠 Absorption Zone (price down, vol up)': 2,
        '🚩 Potential Distribution': -2,
        '💀 Low-volume Dump': -3
    }
    signal_df['score'] = signal_df['signal'].map(priority)
    signal_df = signal_df.sort_values(by='score', ascending=False)
    return signal_df

# === Step 5: Wrapper Function ===
def run_smart_money_analysis(csv_path):
    df = load_and_prepare_data(csv_path)
    df = add_price_tiers(df)
    signal_df = analyze_behavior(df)
    ranked = rank_signals(signal_df)
    return ranked

# === Example ===
if __name__ == "__main__":
    result = run_smart_money_analysis("your_equity_data.csv")
    print(result.head(20))
