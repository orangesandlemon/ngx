# === File: sweden/analyser_se.py ===

import sqlite3
import pandas as pd
from datetime import datetime
import smtplib
import os
from dotenv import load_dotenv
from collections import Counter

# === CONFIG ===
DB_PATH = "data/omx_equities.db"

# === Connect and Load ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by=["name", "date"])

# === Add % Change ===
df["change"] = ((df["close"] - df["open"]) / df["open"]) * 100
df["change"] = df["change"].round(2)

# === New Features for Western Behavior ===
df["ma20"] = df.groupby("name")["close"].transform(lambda x: x.rolling(20, min_periods=5).mean())
df["range"] = df["high"] - df["low"]
df["range_compression"] = df.groupby("name")["range"].transform(lambda x: x.rolling(5, min_periods=3).std().lt(0.5 * x.std()))
df["quiet_green_days"] = df.groupby("name")["close"].transform(lambda x: x.diff().gt(0).rolling(5, min_periods=1).sum())

conn.execute("""
CREATE TABLE IF NOT EXISTS signals (
    name TEXT,
    date TEXT,
    signal TEXT,
    confidence_score REAL,
    volume REAL,
    close REAL,
    change REAL,
    action TEXT,
    buy_range TEXT,
    explanation TEXT,
    limit_up_streak INTEGER,
    signal_tier TEXT,
    volume_uptrend BOOLEAN,
    inst_accum_30 INTEGER,
    stealth_accum BOOLEAN
)
""")

# === Load memory (last 3 days) for each stock ===
signal_memory = {}
try:
    memory_df = pd.read_sql("SELECT name, date, signal FROM signals ORDER BY date DESC LIMIT 500", conn)
    for _, row in memory_df.iterrows():
        name = row["name"]
        signal_memory.setdefault(name, []).append(row["signal"])
except Exception as e:
    print("⚠️ Could not load memory from DB:", e)

def format_reason_text(reasons, row):
    expl = []
    if any("Limit-Up" in r for r in reasons):
        days = row.get("limit_up_streak", 1)
        expl.append(f"The stock has hit its daily 10% limit for {int(days)} consecutive day(s).")
    if "Volume Clustering" in reasons:
        expl.append("Repeated volume spikes detected — institutional loading likely.")
    if "Volume Spike" in reasons or "Moon Volume" in reasons:
        expl.append("Today's volume surged above normal levels.")
    if "Breakout Above Recent Range" in reasons:
        expl.append("Price broke above recent 5-day high — potential breakout underway.")
    if "Higher High & Higher Low" in reasons:
        expl.append("Uptrend confirmed — forming stronger highs and lows.")
    if "Gap Up with Volume" in reasons:
        expl.append("Opened significantly higher with strong volume.")
    if "Weak Candle" in reasons and "Volume Spike" in reasons:
        expl.append("Big volume but small price move — accumulation in progress.")
    if "Price Up" in reasons and "Volume Spike" in reasons:
        expl.append("Strong bullish move with high demand.")
    if "Price Down" in reasons:
        expl.append("Price declined — caution or exit may be needed.")
    if "15-Day Volume Uptrend" in reasons:
        expl.append("Sustained 15-day volume uptrend. Strong interest building.")
    if "10 of 30 Days Institutional Pattern" in reasons:
        expl.append("Institutional accumulation footprint seen 10+ times in last 30 days.")
    if "Stealth Accumulation (Volume Up, Price Flat)" in reasons:
        expl.append("Volume rising steadily, but price stayed flat — signs of smart entry.")
    if "volume_uptrend" in row and row["volume_uptrend"]:
        expl.append("15-day volume uptrend in progress.")
    if "inst_accum_30" in row and row["inst_accum_30"] >= 10:
        expl.append("Institutional footprint over 30 days.")
    if "stealth_accum" in row and row["stealth_accum"]:
        expl.append("Stealth accumulation detected.")
    if "Previously detected setup now showing stronger behavior." in reasons:
        expl.append("This stock was previously flagged. Now showing stronger momentum — time to scale in.")
    if "Full institutional move confirmed." in reasons:
        expl.append("Full bullish confirmation. Strong buy signal.")
    if "Repeated weak signals — likely noise." in reasons:
        expl.append("Several weak alerts without follow-through. Likely false positive.")
    if not expl:
        return ", ".join(reasons)
    return " ".join(expl)

def smart_score(row):
    score = 0
    reasons = []
    signal = None
    action = None

    if row["close"] > row["open"] * 1.01:
        score += 15
        reasons.append("Price Up")
    elif row["close"] < row["open"] * 0.99:
        score -= 10
        reasons.append("Price Down")

    vol_ratio = row["volume"] / (row["volume_5_avg"] + 1e-6)
    if 1.2 <= vol_ratio <= 1.6:
        score += 15
        reasons.append("Moderate Volume Rise")
    elif vol_ratio > 2:
        score -= 5

    body = abs(row["close"] - row["open"])
    range_ = row["high"] - row["low"] + 1e-6
    body_ratio = body / range_
    if body_ratio > 0.6:
        score += 5
        reasons.append("Strong Candle")
    elif body_ratio < 0.3:
        reasons.append("Weak Candle")

    if row.get("stealth_accum", False):
        score += 25
        reasons.append("Stealth Accumulation")
    if row.get("volume_uptrend", False):
        score += 15
        reasons.append("Volume Uptrend (15d)")
    if row.get("inst_accum_30", 0) >= 12:
        score += 20
        reasons.append("Institutional Pattern Detected")

    if abs(row["close"] - row["ma20"]) / row["ma20"] < 0.02:
        score += 10
        reasons.append("Price hugging 20-day MA")
    if row.get("range_compression", False):
        score += 10
        reasons.append("Price compression in last 5 days")
    if row.get("quiet_green_days", 0) >= 3:
        score += 10
        reasons.append("3+ Quiet Green Days")

    memory = row.get("memory", [])
    recent = memory[-3:] if memory else []
    if "👀 Watchlist Setup" in recent and row.get("volume_uptrend", False):
        score += 10
        signal = "⚠️ Setup Detected"
        action = "WATCH"
        reasons.append("Setup progressing with volume trend")

    if not signal:
        if score >= 80:
            signal = "💥 Confirmed Accumulation"
            action = "BUY"
        elif score >= 65:
            signal = "⚠️ Setup Detected"
            action = "WATCH"
        elif score >= 50:
            signal = "👀 Watchlist Setup"
            action = "WATCH"

    return score, reasons, signal, action




# === SIGNALS LOGIC ===
signals = []

for name, group in df.groupby("name"):
    group = group.sort_values("date").copy()

    # ✅ Determine lookback window based on data length
    lookback = 5 if len(group) >= 5 else 3

            # === 🔁 Load Memory From DB
    memory_cursor = conn.cursor()
    memory_cursor.execute("CREATE TABLE IF NOT EXISTS signal_memory (name TEXT, last_signal TEXT, last_action TEXT, last_close REAL, last_high5 REAL, date TEXT)")
    memory_cursor.execute("SELECT last_signal, last_action, last_close, last_high5 FROM signal_memory WHERE name = ?", (name,))
    memory_row = memory_cursor.fetchone()

    last_signal, last_action, last_close, last_high5 = (memory_row if memory_row else (None, None, None, None))

    group["memory"] = [signal_memory.get(name, [])] * len(group)
    
    

    # === Rolling Calculations with dynamic lookback ===
    group["volume_5_avg"] = group["volume"].rolling(lookback, min_periods=1).mean()
    group["is_spike"] = group["volume"] > 1.5 * group["volume_5_avg"]
    group["spike_count_5"] = group["is_spike"].rolling(lookback, min_periods=1).sum()

    
    group["price_5_high"] = group["high"].rolling(lookback, min_periods=1).max()
    group["low_5"] = group["low"].rolling(lookback, min_periods=1).min()
    
    # === 📈 Trend Features (Longer-Term View) ===
    group["volume_15_avg"] = group["volume"].rolling(15, min_periods=5).mean()
    group["volume_uptrend"] = group["volume_15_avg"] > group["volume_15_avg"].shift(5)

    group["inst_footprint"] = (group["volume"] > group["volume_5_avg"])
    group["inst_accum_30"] = group["inst_footprint"].rolling(30, min_periods=10).sum()

    group["price_change_15"] = group["close"].pct_change(periods=15)
    group["volume_change_15"] = group["volume"].pct_change(periods=15)
    group["stealth_accum"] = (group["volume_change_15"] > 0.3) & (group["price_change_15"] < 0.05)


    # === Daily Comparisons ===
    group["prev_high"] = group["high"].shift(1)
    group["prev_low"] = group["low"].shift(1)
    group["previous_close"]= group["close"].shift(1)

    group["higher_high"] = group["high"] > group["prev_high"]
    group["higher_low"] = group["low"] > group["prev_low"]

    group["gap_up"] = group["open"] > group["previous_close"] * 1.02
    group["gap_down"] = group["open"] < group["previous_close"] * 0.98

    group["limit_up"] = (
        (group["close"] - group["previous_close"]) / group["previous_close"]
    ).round(4) >= 0.099

    group["limit_up_streak"] = group["limit_up"].astype(int)
    group["limit_up_streak"] = group["limit_up_streak"].groupby(
        (group["limit_up_streak"] != group["limit_up_streak"].shift()).cumsum()
    ).cumsum()
    group.loc[~group["limit_up"], "limit_up_streak"] = 0


    # === Process each row ===
    for i in range(len(group)):
        row = group.iloc[i]
        if pd.isna(row["volume_5_avg"]):
            continue

        # ⬅️ Signal scoring logic begins
        score, reasons, signal, action = smart_score(row)

        # === 🔍 Trend Signal Boosters ===
        if row.get("volume_uptrend", False):
            score += 10
            reasons.append("15-Day Volume Uptrend")

        if row.get("inst_accum_30", 0) >= 10:
            score += 15
            reasons.append("10 of 30 Days Institutional Pattern")

        if row.get("stealth_accum", False):
            score += 20
            reasons.append("Stealth Accumulation (Volume Up, Price Flat)")

        streak = int(row.get("limit_up_streak", 0))

        # === Override for Limit-Up
        if streak >= 1:
            score += 40
            reasons.append(f"Limit-Up Streak: {streak} day(s)")
            if streak == 1:
                signal = "🚨 Limit-Up Watch"
                action = "WATCH"
            elif streak == 2:
                signal = "💡 Limit-Up Accumulation"
                action = "BUY SMALL"
            elif streak >= 3:
                signal = "🚀 Limit-Up Breakout"
                action = "BUY CONFIRMED"

        # === NEW: Retest Memory Logic
        if last_close and abs(row["close"] - last_close) <= 0.02 * last_close:
            score += 5
            reasons.append("Retesting key level")

        if last_action == "BUY" and signal == "WATCH":
            reasons.append("Signal weakening")
            signal = "EXIT"
            action = "EXIT"

        # === Final check before saving
        if signal:
            low_price = min(row["open"], row["close"])
            high_price = max(row["open"], row["close"])
            buy_range = f"₦{low_price:.2f} – ₦{high_price:.2f}" if action and "BUY" in action else "—"

            signals.append({
                "name": row["name"],
                "date": row["date"].strftime("%Y-%m-%d"),
                "signal": signal,
                "confidence_score": score,
                "volume": row["volume"],
                "close": row["close"],
                "change": row["change"],
                "action": action,
                "buy_range": buy_range,
                "explanation": format_reason_text(reasons, row),
                "limit_up_streak": streak,
                "signal_tier": "confirmed" if score >= 75 else "setup" if score >= 60 else "watchlist" if score >= 40 else "none",
                "volume_uptrend": row.get("volume_uptrend", False),
                "inst_accum_30": row.get("inst_accum_30", 0),
                "stealth_accum": row.get("stealth_accum", False),
            })

            memory_cursor.execute("""
                INSERT OR REPLACE INTO signal_memory (name, last_signal, last_action, last_close, last_high5, date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row["name"],
                signal,
                action,
                row["close"],
                row.get("price_5_high", row["high"]),
                row["date"].strftime("%Y-%m-%d")
            ))

# === SAVE SIGNALS TO DB ===
signals_df = pd.DataFrame(signals)
if not signals_df.empty:
    unique_dates = signals_df["date"].unique()
    for d in unique_dates:
        conn.execute("DELETE FROM signals WHERE date = ?", (d,))
    signals_df.drop(columns=["open"], inplace=True, errors="ignore")
    signals_df["date"] = pd.to_datetime(signals_df["date"]).dt.strftime("%Y-%m-%d")
    signals_df.to_sql("signals", conn, if_exists="append", index=False)

    print(f"✅ {len(signals_df)} signals stored in 'signals' table.")
    print(signals_df.head(3))
else:
    print("⚠️ No signals found (need more days of data).")

conn.close()
