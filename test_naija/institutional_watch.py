# === File: institutional_watch_se.py ===
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "data/ngx_equities.db"
TABLE_NAME = "equities"
SUMMARY_CSV = "institutional_watch.csv"
HISTORY_CSV = "institutional_watch_history.csv"
LOOKBACK_DAYS = 30

# === Load Data ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
conn.close()

# Clean and prepare
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by=["name", "date"])
latest_date = df["date"].max()
cutoff = latest_date - timedelta(days=LOOKBACK_DAYS)
df = df[df["date"] >= cutoff]

history_rows = []
memory = {}

for name, group in df.groupby("name"):
    group = group.copy()
    group["volume_ma"] = group["volume"].rolling(window=10, min_periods=1).mean()
    group["price_change"] = group["close"] - group["open"]
    group["low_shift"] = group["low"].shift(1)
    group["higher_low"] = group["low"] > group["low_shift"]
    group["p_up_v_down"] = (group["price_change"] > 0) & (group["volume"] < group["volume_ma"])

    memory[name] = {
        "stealth_streak": 0,
        "buildup_streak": 0,
        "confirmed": False,
        "vol_down_streak": 0,
        "price_up_days": 0
    }

    for i, row in group.iterrows():
        score = 0
        signal = ""
        reasons = []

        if row["volume"] > 1.2 * row["volume_ma"] and row["price_change"] >= 0:
            score += 1
            reasons.append("Vol up, price up or flat")

        if row["higher_low"]:
            score += 1
            reasons.append("Higher low")

        if row["volume"] > 1.5 * row["volume_ma"]:
            score += 1
            reasons.append("Volume surge")

        if row["p_up_v_down"]:
            memory[name]["vol_down_streak"] += 1
            memory[name]["price_up_days"] += 1
        else:
            memory[name]["vol_down_streak"] = 0
            if row["price_change"] > 0:
                memory[name]["price_up_days"] += 1
            else:
                memory[name]["price_up_days"] = 0

        if memory[name]["vol_down_streak"] >= 3 and memory[name]["price_up_days"] >= 4:
            score += 1
            reasons.append("Dry-up accumulation (P↑ V↓)")

        if score >= 2:
            memory[name]["stealth_streak"] += 1
            signal = "watchlist"

            if memory[name]["stealth_streak"] >= 3:
                signal = "buildup"
                memory[name]["buildup_streak"] += 1

            if memory[name]["buildup_streak"] >= 2:
                signal = "buy"
                memory[name]["confirmed"] = True

        else:
            if memory[name]["confirmed"] and row["price_change"] < -3:
                signal = "sell"
                reasons.append("Reversal after buy")
                memory[name] = {
                    "stealth_streak": 0,
                    "buildup_streak": 0,
                    "confirmed": False,
                    "vol_down_streak": 0,
                    "price_up_days": 0
                }
            else:
                memory[name]["stealth_streak"] = 0
                memory[name]["buildup_streak"] = 0

        tier = {
            "watchlist": "watchlist",
            "buildup": "buildup",
            "buy": "buy",
            "sell": "sell"
        }.get(signal, "")

        history_rows.append({
            "name": name,
            "date": row["date"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "volume_ma": row["volume_ma"],
            "price_change": row["price_change"],
            "signal_score": score,
            "signal": signal,
            "tier": tier,
            "action_reason": ", ".join(reasons)
        })

# === Save history ===
history_df = pd.DataFrame(history_rows)
history_df.to_csv(HISTORY_CSV, index=False)

# === Build summary ===
summary_df = (
    history_df[history_df["signal"] != ""]
    .sort_values("date")
    .groupby("name")
    .agg(
        stealth_days=("signal", lambda x: (x == "watchlist").sum()),
        buildup_days=("signal", lambda x: (x == "buildup").sum()),
        buy_days=("signal", lambda x: (x == "buy").sum()),
        sell_days=("signal", lambda x: (x == "sell").sum()),
        first_signal=("date", "min"),
        last_signal=("signal", lambda x: x.iloc[-1]),
        signal_score=("signal_score", "sum"),
        last_date=("date", "max")
    )
    .reset_index()
)

summary_df["tier"] = summary_df["last_signal"].map({
    "watchlist": "watchlist",
    "buildup": "buildup",
    "buy": "buy",
    "sell": "sell"
}).fillna("unknown")

summary_df = summary_df.sort_values(by=["last_signal", "signal_score"], ascending=[True, False])
summary_df.to_csv(SUMMARY_CSV, index=False)

# Save to DB
with sqlite3.connect(DB_PATH) as conn:
    summary_df.to_sql("institutional_watch", conn, if_exists="replace", index=False)
    history_df.to_sql("institutional_watch_history", conn, if_exists="replace", index=False)

print(f"✅ Saved {len(summary_df)} summary entries and {len(history_df)} history rows.")
