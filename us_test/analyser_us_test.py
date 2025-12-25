# analyser.py
import sqlite3
import pandas as pd
from datetime import datetime
import smtplib
import os
from dotenv import load_dotenv
from collections import Counter

# === CONFIG ===
DB_PATH = "data/us_equities.db"

# Skip if it's Saturday (5) or Sunday (6)
# if datetime.today().weekday() >= 5:
#   print("🛑 Market closed (weekend). Exiting.")

# exit()
# === LOAD DATA ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities_100 WHERE close <= 12 ", conn)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by=["name", "date"])

# ✅ TRUE % change = (Close - Previous Close) / Previous Close
df["change"] = ((df["close"] - df["previous_close"]) / df["previous_close"]) * 100
df["change"] = df["change"].round(2)

# === MARKET SUMMARY ===
latest_day = df["date"].max()
prev_day = latest_day - pd.Timedelta(days=1)

today_df = df[df["date"] == latest_day].copy()
prev_df = df[df["date"] == prev_day]


# === Load memory (last 3 days) for each stock ===
signal_memory_100 = {}
try:
    memory_df = pd.read_sql(
        "SELECT name, date, signal FROM signals ORDER BY date DESC LIMIT 500", conn
    )
    for _, row in memory_df.iterrows():
        name = row["name"]
        signal_memory_100.setdefault(name, []).append(row["signal"])
except Exception as e:
    print("⚠️ Could not load memory from DB:", e)


def format_reason_text(reasons, row):
    expl = []

    if any("Limit-Up" in r for r in reasons):
        days = row.get("limit_up_streak", 1)
        expl.append(
            f"The stock has hit its daily 10% limit for {int(days)} consecutive day(s)."
        )

    if "Volume Clustering" in reasons:
        expl.append("Repeated volume spikes detected — institutional loading likely.")

    if "Volume Spike" in reasons or "Moon Volume" in reasons:
        expl.append("Today's volume surged above normal levels.")

    if "Breakout Above Recent Range" in reasons:
        expl.append(
            "Price broke above recent 5-day high — potential breakout underway."
        )

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
        expl.append(
            "Institutional accumulation footprint seen 10+ times in last 30 days."
        )

    if "Stealth Accumulation (Volume Up, Price Flat)" in reasons:
        expl.append(
            "Volume rising steadily, but price stayed flat — signs of smart entry."
        )

    if "volume_uptrend" in row and row["volume_uptrend"]:
        expl.append("15-day volume uptrend in progress.")

    if "inst_accum_30" in row and row["inst_accum_30"] >= 10:
        expl.append("Institutional footprint over 30 days.")

    if "stealth_accum" in row and row["stealth_accum"]:
        expl.append("Stealth accumulation detected.")

    if "Previously detected setup now showing stronger behavior." in reasons:
        expl.append(
            "This stock was previously flagged. Now showing stronger momentum — time to scale in."
        )

    if "Full institutional move confirmed." in reasons:
        expl.append("Full bullish confirmation. Strong buy signal.")

    if "Repeated weak signals — likely noise." in reasons:
        expl.append(
            "Several weak alerts without follow-through. Likely false positive."
        )

    if not expl:
        return ", ".join(reasons)  # fallback

    return " ".join(expl)


def smart_score(row):
    score = 0
    reasons = []
    signal = None
    action = None

    # === Price action ===
    if row["close"] > row["open"]:
        score += 10
        reasons.append("Price Up")
    elif row["close"] < row["open"]:
        score -= 5
        reasons.append("Price Down")

    # === Volume vs average ===
    if row["volume"] > 1.5 * row["volume_5_avg"]:
        score += 15
        reasons.append("Volume Spike")
    elif row["volume"] < 0.5 * row["volume_5_avg"]:
        score -= 10

    # === Candle shape ===
    body = abs(row["close"] - row["open"])
    range_ = row["high"] - row["low"] + 1e-6
    body_strength = body / range_
    if body_strength > 0.6:
        score += 8
        reasons.append("Strong Candle")
    elif body_strength < 0.3:
        score -= 5
        reasons.append("Weak Candle")

    # === Stealth accumulation pattern ===
    if row.get("stealth_accum", False):
        score += 25
        reasons.append("Stealth Accumulation (Volume Up, Price Flat)")

    # === Multi-day stealth buildup ===
    if row.get("stealth_streak", 0) >= 2:
        score += 10
        reasons.append("Multi-day stealth accumulation")

    # === Combo behavior ===
    if "Price Up" in reasons and "Volume Spike" in reasons:
        reasons.append("Stealth Accumulation")
        score += 15
        signal = "Institutional Accumulation"
        action = "BUY"
    elif "Weak Candle" in reasons and "Volume Spike" in reasons:
        reasons.append("Accumulation Under Cover")
        score += 15
    elif "Volume Spike" in reasons and "High Trade Count" in reasons:
        reasons.append("Retail Buying Frenzy")
        score += 10
        signal = "Retail Buying Frenzy"
        action = "AVOID"

    # === 🧠 Memory-Based Signal Progression Logic ===
    memory = row.get("memory", [])
    recent_signals = memory[-3:] if memory else []

    # 1️⃣ Progressing setup → BUY
    if "⚠️ Setup Detected" in recent_signals and signal == "👀 Watchlist Setup":
        signal = "📈 Progressing Setup"
        action = "BUY SMALL"
        reasons.append("Previously detected setup is strengthening — time to scale in.")

    if (
        "📈 Progressing Setup" in recent_signals
        and signal == "Institutional Accumulation"
    ):
        signal = "💥 Institution Moon Alert"
        action = "BUY CONFIRMED"
        reasons.append("Full institutional move confirmed.")

    # 3️⃣ Avoid Decaying Setup
    if (
        signal in ["👀 Watchlist Setup", "⚠️ Setup Detected"]
        and "👀 Watchlist Setup" in recent_signals
        and "⚠️ Setup Detected" in recent_signals
    ):
        score -= 20
        reasons.append("Repeated weak signals — likely noise.")
        action = "AVOID"

    # 4️⃣ Handle potential retest at key level (if signal re-emerges with better volume)
    if signal == "Institutional Accumulation" and "BUY" not in str(action):
        if (
            "⚠️ Setup Detected" in recent_signals
            or "👀 Watchlist Setup" in recent_signals
        ):
            reasons.append("Signal re-emerging — possible retest.")
            score += 10
            action = "BUY SMALL"

    # === Signal tier fallback ===
    if not signal:
        if score >= 70:
            signal = "Institutional Accumulation"
            action = "BUY"
        elif score >= 55:
            signal = "⚠️ Setup Detected"
            action = "WATCH"
        elif score >= 35:
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
    memory_cursor.execute(
        "CREATE TABLE IF NOT EXISTS signal_memory_100 (name TEXT, last_signal TEXT, last_action TEXT, last_close REAL, last_high5 REAL, date TEXT)"
    )
    memory_cursor.execute(
        "SELECT last_signal, last_action, last_close, last_high5 FROM signal_memory_100 WHERE name = ?",
        (name,),
    )
    memory_row = memory_cursor.fetchone()

    last_signal, last_action, last_close, last_high5 = (
        memory_row if memory_row else (None, None, None, None)
    )

    group["memory"] = [signal_memory_100.get(name, [])] * len(group)

    # === Rolling Calculations with dynamic lookback ===
    group["volume_5_avg"] = group["volume"].rolling(lookback, min_periods=1).mean()
    group["is_spike"] = group["volume"] > 1.5 * group["volume_5_avg"]
    group["spike_count_5"] = group["is_spike"].rolling(lookback, min_periods=1).sum()

    group["price_5_high"] = group["high"].rolling(lookback, min_periods=1).max()
    group["low_5"] = group["low"].rolling(lookback, min_periods=1).min()

    # === 📈 Trend Features (Longer-Term View) ===
    group["volume_15_avg"] = group["volume"].rolling(15, min_periods=5).mean()
    group["volume_uptrend"] = group["volume_15_avg"] > group["volume_15_avg"].shift(5)

    group["inst_footprint"] = group["volume"] > group["volume_5_avg"]
    group["inst_accum_30"] = group["inst_footprint"].rolling(30, min_periods=10).sum()

    group["price_change_15"] = group["close"].pct_change(periods=15)
    group["volume_change_15"] = group["volume"].pct_change(periods=15)
    group["stealth_accum"] = (group["volume_change_15"] > 0.3) & (
        group["price_change_15"] < 0.05
    )

    # === Daily Comparisons ===
    group["prev_high"] = group["high"].shift(1)
    group["prev_low"] = group["low"].shift(1)

    group["higher_high"] = group["high"] > group["prev_high"]
    group["higher_low"] = group["low"] > group["prev_low"]

    group["gap_up"] = group["open"] > group["previous_close"] * 1.02
    group["gap_down"] = group["open"] < group["previous_close"] * 0.98

    group["limit_up"] = group["change"] >= 7  # 7% gain day = strong bullish move

    group["limit_up_streak"] = group["limit_up"].astype(int)
    group["limit_up_streak"] = (
        group["limit_up_streak"]
        .groupby(
            (group["limit_up_streak"] != group["limit_up_streak"].shift()).cumsum()
        )
        .cumsum()
    )
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
            score += 0
            reasons.append("Stealth Accumulation (Volume Up, Price Flat)")

        streak = int(row.get("limit_up_streak", 0))

        # === Override for Limit-Up
        if streak >= 1:
            score += 25
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
            buy_range = (
                f"${low_price:.2f} – ${high_price:.2f}"
                if action and "BUY" in action
                else "—"
            )

            signals.append(
                {
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
                    "signal_tier": (
                        "confirmed"
                        if score >= 70
                        else (
                            "setup"
                            if score >= 55
                            else "watchlist" if score >= 35 else "none"
                        )
                    ),
                    "volume_uptrend": row.get("volume_uptrend", False),
                    "inst_accum_30": row.get("inst_accum_30", 0),
                    "stealth_accum": row.get("stealth_accum", False),
                }
            )

            memory_cursor.execute(
                """
                INSERT OR REPLACE INTO signal_memory_100 (name, last_signal, last_action, last_close, last_high5, date)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    row["name"],
                    signal,
                    action,
                    row["close"],
                    row.get("price_5_high", row["high"]),
                    row["date"].strftime("%Y-%m-%d"),
                ),
            )

# === SAVE SIGNALS TO DB ===
signals_df = pd.DataFrame(signals)
if not signals_df.empty:
    unique_dates = signals_df["date"].unique()
    for d in unique_dates:
        conn.execute("DELETE FROM signals_100 WHERE date = ?", (d,))
    signals_df.drop(columns=["open"], inplace=True, errors="ignore")
    signals_df["date"] = pd.to_datetime(signals_df["date"]).dt.strftime("%Y-%m-%d")
    signals_df.to_sql("signals_100", conn, if_exists="append", index=False)

    print(f"✅ {len(signals_df)} signals stored in 'signals_100' table.")
    print(signals_df.head(3))
else:
    print("⚠️ No signals found (need more days of data).")

conn.close()
