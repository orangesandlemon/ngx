import sqlite3
import pandas as pd

# === CONFIG ===
DB_PATH = "data/ngx_equities.db"  # Modify if needed
WINDOW = 5
SCORE_THRESHOLD = 3  # Tune this

conn = sqlite3.connect(DB_PATH)

df = pd.read_sql("SELECT * FROM equities", conn)
conn.close()

# If market_cap not present or empty, calculate it
if "shares_outstanding" in df.columns:
    df["market_cap"] = df["close"] * df["shares_outstanding"]
else:
    df["market_cap"] = None


# === Preprocessing ===
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by=["name", "date"])

# === Add market_cap if shares_outstanding is available ===
if "market_cap" not in df.columns and "shares_outstanding" in df.columns:
    df["market_cap"] = df["close"] * df["shares_outstanding"]

# === Core Logic ===
def detect_smart_accumulation(df):
    result = []

    for name, group in df.groupby("name"):
        group = group.sort_values("date").copy()

        # Rolling averages
        group["volume_avg"] = group["volume"].rolling(WINDOW).mean()
        group["close_avg"] = group["close"].rolling(WINDOW).mean()
        group["trade_avg"] = group["trades"].rolling(WINDOW).mean()

        if "market_cap" in group.columns:
            group["marketcap_avg"] = group["market_cap"].rolling(WINDOW).mean()
        else:
            group["marketcap_avg"] = None

        for i in range(WINDOW - 1, len(group)):
            window = group.iloc[i - WINDOW + 1 : i + 1]

            score = 0
            latest = group.iloc[i]

            for _, row in window.iterrows():
                # 1. Flat price near 5-day average
                if abs(row["close"] - row["close_avg"]) <= 0.5:
                    score += 1

                # 2. Volume spike vs 5-day average
                if (row["volume"] > 1.5 * row["volume_avg"] and row["trades"] < 0.7 * row["trade_avg"]):
                    score += 1

                # 3. Dip day (flush or shakeout)
                if row["close"] < row["close_avg"] * 0.98:
                    score += 1

                # 4. Market cap rising steadily (optional)
                if pd.notna(row.get("market_cap")) and pd.notna(row.get("marketcap_avg")):
                    if row["market_cap"] > 1.02 * row["marketcap_avg"]:
                        score += 1

            #accumulation_signal = score >= SCORE_THRESHOLD
            # Tiered signals
                if score >= 4:
                    signal_tier = "🚀 Confirmed Buy"
                elif score == 3:
                    signal_tier = "🤏 Buy Small"
                elif score == 2:
                    signal_tier = "🕵️‍♀️ Watchlist"
                else:
                    signal_tier = None  # No signal this day


            result.append({
                "name": name,
                "date": latest["date"],
                "close": latest["close"],
                "volume": latest["volume"],
                "volume_avg": latest["volume_avg"],
                "market_cap": latest.get("market_cap", None),
                "marketcap_avg": latest.get("marketcap_avg", None),
                "accumulation_score": score,
                "accumulation_signal": score >= SCORE_THRESHOLD,
                "signal_tier": signal_tier
            })


    return pd.DataFrame(result)

# === Run & Save ===
accum_signals = detect_smart_accumulation(df)
accum_signals.to_csv("smart_accumulation_signals_v2.csv", index=False)
print(f"✅ Saved {len(accum_signals)} signals to 'smart_accumulation_signals_v2.csv'")
