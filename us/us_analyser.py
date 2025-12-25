import sqlite3
import pandas as pd
from datetime import datetime
import os

os.makedirs("data", exist_ok=True)
DB_PATH = "data/us_equities.db"

# === Load Data ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by=["name", "date"])

# ✅ Limit df to today and last working day only
# ✅ Load today + last working day as target dates
from datetime import timedelta
today = pd.Timestamp.now().normalize()
yesterday = today - pd.Timedelta(days=1)
weekday = today.weekday()
if weekday == 0:  # Monday
    last_working_day = today - pd.Timedelta(days=3)  # Friday
elif weekday == 1:  # Tuesday
    last_working_day = today - pd.Timedelta(days=4)  # Friday
else:
    last_working_day = yesterday

valid_dates = [today.strftime("%Y-%m-%d"), last_working_day.strftime("%Y-%m-%d")]
print("Valid Dates:", valid_dates)



# ✅ Also load ~5 extra days to allow rolling averages to work
min_date_needed = pd.to_datetime(valid_dates).min() - pd.Timedelta(days=5)
df = df[df["date"] >= min_date_needed]


# Just after df = df[df["date"].isin(...)]

print("🧠 Filtered DataFrame size:", len(df))
print("🗓️ Dates remaining in df:", df["date"].dt.strftime("%Y-%m-%d").unique())
print("📊 Sample rows:\n", df.head())


print("✅ Running signals for:", valid_dates)
#print("📊 Available dates in df:", df["date"].dt.strftime("%Y-%m-%d").unique())


# === Load recent news scores ===
news_df = pd.read_sql("SELECT * FROM news_signals", conn)
news_df["date"] = pd.to_datetime(news_df["date"])

# Keep only news from last 2 days
news_cutoff = datetime.now() - pd.Timedelta(days=2)
recent_news = news_df[news_df['date'] >= news_cutoff]

# Create mapping like {("AAPL", "2025-05-24"): (score, reason)}
news_map = {}
for _, row in recent_news.iterrows():
    key = (row["name"], row["date"].strftime("%Y-%m-%d"))
    news_map[key] = (row["news_score"], row["news_reason"])


signals = []


# === News Fetch Helper ===
def get_recent_news_score(conn, symbol, date):
    query = """
        SELECT news_score, news_reason
        FROM news_signals
        WHERE name = ? AND date = ?
    """
    cursor = conn.cursor()
    cursor.execute(query, (symbol, date.strftime("%Y-%m-%d")))
    results = cursor.fetchall()

    if not results:
        return 0, None

    total_score = sum([r[0] for r in results])
    combined_reason = "; ".join([r[1] for r in results if r[1]])
    return total_score, combined_reason

# === Inference Function ===
def infer_expected_option_type(row, reasons):
    if "Price Up" in reasons and "Low Trades" in reasons:
        return "Call"
    if "Price Down" in reasons and "High Trade Count" in reasons:
        return "Put"
    if "Volume Spike" in reasons and "Momentum Reversal" in reasons:
        return "Put"
    if "Volume Acceleration" in reasons and "Strong Candle" in reasons:
        return "Call"
    if "Weak Candle" in reasons and "Volume Spike" in reasons:
        return "Put"
    if "Strong Candle" in reasons and "Price Up" in reasons:
        return "Call"
    return None

# === Signal Logic ===
for name, group in df.groupby("name"):
    group = group.sort_values("date")
    group["change"] = group["close"] - group["open"]
    group["avg_volume_5"] = group["volume"].rolling(5).mean()
    group["is_spike"] = group["volume"] > 1.5 * group["avg_volume_5"]   # Flag volume spikes
    group["spike_count_5"] = group["is_spike"].rolling(5).sum()  # Count how many spikes in past 5 days
    group["trades"] = group["volume"] // 100  # Simulated
    group["avg_trades_5"] = group["trades"].rolling(5).mean()
    group["value"] = group["close"] * group["volume"]
    
    
    for i in range(len(group)):
        row = group.iloc[i]
        if pd.isna(row["avg_volume_5"]):
            # Skip if we don't have enough volume history
            continue

        # ✅ Only generate signal for valid dates
        if row["date"].strftime("%Y-%m-%d") not in valid_dates:
            continue


        # === Metrics ===
        volume_accel = group["volume"].diff().rolling(3).mean().iloc[i]
        body_strength = abs(row["close"] - row["open"]) / (row["high"] - row["low"] + 1e-6)
        rolling_change = group["close"].pct_change().rolling(3).mean().iloc[i]

        # === Classification ===
        signal = None
        action = None
        option_type = None
        expected_option_type = None
        buy_range = None
        reasons = []
        score = 0

               
        # === Score and Tag ===
        if row["close"] > row["open"]:
            score += 20
            reasons.append("Price Up")
        elif row["close"] < row["open"]:
            score += 20
            reasons.append("Price Down")

        if row["volume"] > 1.5 * row["avg_volume_5"]:
            score += 30
            reasons.append("Volume Spike")
       
        if row["spike_count_5"] >= 3:
            score += 25
            reasons.append("Volume Clustering (3 spikes in 5 days)")
          
        if volume_accel and volume_accel > 1e6:
            score += 20
            reasons.append("Volume Acceleration")

        if row["trades"] < row["avg_trades_5"]:
            score += 20
            reasons.append("Low Trades")
        elif row["trades"] > 2 * row["avg_trades_5"]:
            score += 20
            reasons.append("High Trade Count")

        if body_strength > 0.6:
            score += 20
            reasons.append("Strong Candle")
        elif body_strength < 0.3:
            score -= 10
            reasons.append("Weak Candle")

        if rolling_change and rolling_change < 0:
            score += 10
            reasons.append("Momentum Reversal")
            
         # News sentiment
        news_score = 0
        news_reason = None
        for offset in range(0, 3):  # include news up to 2 days ahead
            news_key = (row["name"], (row["date"] + pd.Timedelta(days=offset)).strftime("%Y-%m-%d"))
            if news_key in news_map:
                news_score, news_reason = news_map[news_key]
                break

        if news_score != 0:
            score += news_score
            reasons.append(f"News Impact ({'+' if news_score > 0 else ''}{news_score})")
            if news_reason:
                reasons.append(f"News: {news_reason[:100]}...")

        # === Tier ===
        score = max(0, min(score, 100))
        print(f"DEBUG: {row['name']} on {row['date'].strftime('%Y-%m-%d')} | score={score} | reasons={reasons}")

        tier = "confirmed" if score >= 75 else "setup" if score >= 60 else None
        if not tier:
            continue
        
        # Guarantee a signal is assigned
        if tier == "setup":
            signal = "⚠️ Setup Detected"
            action = "WATCH"
            expected_option_type = infer_expected_option_type(row, reasons)
        elif tier == "confirmed":
            if "Price Up" in reasons and "Volume Spike" in reasons and "Low Trades" in reasons:
                signal = "Institutional Accumulation"
                action = "BUY CALL"
                option_type = "Call"
            elif "Price Down" in reasons and "Volume Spike" in reasons:
                signal = "Distribution Exit"
                action = "BUY PUT"
                option_type = "Put"
            elif "Price Up" in reasons and "High Trade Count" in reasons:
                signal = "Retail Buying Frenzy"
                action = "AVOID"

        
        from datetime import timedelta

        # Determine expiry date suggestion
        base_expiry = row["date"]  # Date of the signal
        if tier == "confirmed":
            expiry_date = base_expiry + timedelta(days=10)
        else:
            expiry_date = base_expiry + timedelta(days=14)

        # Snap to next Friday
        while expiry_date.weekday() != 4:  # 0=Mon, ..., 4=Friday
            expiry_date += timedelta(days=1)

           

            # === Try to infer expected option direction
            expected_option_type = None

            if row["close"] > row["open"]:
                expected_option_type = "Call"
            elif row["close"] < row["open"]:
                expected_option_type = "Put"
            elif "Momentum Reversal" in reasons:
                if row["change"] > 0:
                    expected_option_type = "Call"
                else:
                    expected_option_type = "Put"
            elif "Strong Candle" in reasons:
                if body_strength > 0.6 and row["close"] > row["open"]:
                    expected_option_type = "Call"
                elif body_strength > 0.6 and row["close"] < row["open"]:
                    expected_option_type = "Put"

        # === Infer Option Type Even for Setups ===
        expected_option_type = infer_expected_option_type(row, reasons)

        # === Option Estimates ===
        low_price = min(row["open"], row["close"])
        high_price = max(row["open"], row["close"])
        buy_range = f"${low_price:.2f} – ${high_price:.2f}"
        strike_price = round(row["close"] * 0.98 if action == "BUY CALL" else row["close"] * 1.02, 2)
        option_entry = 0.50  # placeholder
        option_target = round(option_entry * 1.8, 2)

        if not signal and tier:
            signal = f"{tier.title()} - Generic"
            action = "REVIEW"

       
        if signal:
            print(f"➡️ {row['name']} on {row['date'].strftime('%Y-%m-%d')} - Score: {score} - Tier: {tier}")
            print(f"✅ ADDING SIGNAL: {row['name']} | {row['date'].strftime('%Y-%m-%d')} | Score: {score} | Tier: {tier} | Signal: {signal}")

            signals.append({
                "name": row["name"],
                "date": row["date"].strftime("%Y-%m-%d"),
                "signal": signal,
                "confidence_score": score,
                "volume": row["volume"],
                "trades": row["trades"],
                "value": row["value"],
                "close": row["close"],
                "change": row["change"],
                "action": action,
                "option_type": option_type,
                "expected_option_type": expected_option_type,
                "buy_range": buy_range,
                "explanation": ", ".join(reasons),
                "option_strike": strike_price,
                "option_expiry": expiry_date.strftime("%Y-%m-%d"),
                "option_entry": option_entry,
                "option_target": option_target,
                "signal_tier": tier
            })
# === Save Signals ===

signals_df = pd.DataFrame(signals)

#print("📅 Unique signal dates before filtering:", signals_df["date"].unique())

if not signals_df.empty:
    
    # === Delete existing signals for only these dates
    for d in valid_dates:
        conn.execute("DELETE FROM signals_us WHERE date = ?", (d,))

    # === Save new signals
    signals_df.to_sql("signals_us", conn, if_exists="append", index=False)
    print(f"✅ {len(signals_df)} signals saved for {valid_dates}.")
else:
    print("⚠️ No strong signals for today or previous working day.")



conn.close()
