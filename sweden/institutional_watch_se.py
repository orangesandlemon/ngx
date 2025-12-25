# === File: institutional_watch_se.py ===
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "data/omx_equities.db"
TABLE_NAME = "equities"
SUMMARY_CSV = "institutional_watch_se.csv"
HISTORY_CSV = "institutional_watch_history.csv"
LOOKBACK_DAYS = 90
SIGNAL_EXPIRATION_DAYS = 15

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
    group["prev_close"] = group["close"].shift(1)

    memory[name] = {
        "stealth_streak": 0,
        "buildup_streak": 0,
        "confirmed": False,
        "vol_down_streak": 0,
        "price_up_days": 0,
        "days_since_buy": 0,
        "first_signal_date": None,
        "last_signal": ""
    }

    for i, row in group.iterrows():
        score = 0
        signal = ""
        reasons = []

        if row["volume"] > 1.1 * row["volume_ma"] and row["price_change"] >= 0:
            score += 1
            reasons.append("Vol up, price up or flat")

        if row["higher_low"]:
            score += 1
            reasons.append("Higher low")

        if row["volume"] > 1.4 * row["volume_ma"]:
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

        # Reset expired signals
        if memory[name]["first_signal_date"] and (row["date"] - memory[name]["first_signal_date"]).days > SIGNAL_EXPIRATION_DAYS:
            memory[name] = {
                "stealth_streak": 0,
                "buildup_streak": 0,
                "confirmed": False,
                "vol_down_streak": 0,
                "price_up_days": 0,
                "days_since_buy": 0,
                "first_signal_date": None,
                "last_signal": ""
            }

        if score >= 1:
            if memory[name]["first_signal_date"] is None or memory[name]["last_signal"] in ["", "sell"]:
                memory[name]["first_signal_date"] = row["date"]

            memory[name]["stealth_streak"] += 1
            signal = "watchlist"

            if memory[name]["stealth_streak"] >= 2:
                signal = "buildup"
                memory[name]["buildup_streak"] += 1

            if memory[name]["buildup_streak"] >= 1:
                signal = "buy"
                memory[name]["confirmed"] = True
                memory[name]["days_since_buy"] = 0
        else:
            if memory[name]["confirmed"] and row["prev_close"] and row["close"] < 0.95 * row["prev_close"]:
                signal = "sell"
                reasons.append("5% drop after buy confirmation")
                memory[name] = {
                    "stealth_streak": 0,
                    "buildup_streak": 0,
                    "confirmed": False,
                    "vol_down_streak": 0,
                    "price_up_days": 0,
                    "days_since_buy": 0,
                    "first_signal_date": None,
                    "last_signal": ""
                }
            else:
                memory[name]["stealth_streak"] = max(0, memory[name]["stealth_streak"] - 1)
                memory[name]["buildup_streak"] = max(0, memory[name]["buildup_streak"] - 1)

        if memory[name]["confirmed"]:
            memory[name]["days_since_buy"] += 1

        memory[name]["last_signal"] = signal

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

# === Build Timeline Table (signal transitions) ===
timeline_rows = []

for name, group in history_df.sort_values(["name", "date"]).groupby("name"):
    prev_signal = ""
    last_change_date = None

    for _, row in group.iterrows():
        current_signal = row["signal"]
        date = row["date"]

        # When signal changes (including from '' → watchlist)
        if current_signal != "" and current_signal != prev_signal:
            timeline_rows.append({
                "name": name,
                "date": date,
                "signal": current_signal
            })
            prev_signal = current_signal
            last_change_date = date

        # Reset logic: if expired
        if last_change_date and (date - pd.to_datetime(last_change_date)).days > SIGNAL_EXPIRATION_DAYS:
            timeline_rows.append({
                "name": name,
                "date": date,
                "signal": "reset (expired)"
            })
            prev_signal = ""
            last_change_date = None

        # Reset logic: after sell
        if current_signal == "sell":
            prev_signal = ""
            last_change_date = None

timeline_df = pd.DataFrame(timeline_rows)
timeline_df.to_csv("institutional_signal_timeline.csv", index=False)


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

# Drop rows where the last signal is too old
valid_cutoff = latest_date - timedelta(days=SIGNAL_EXPIRATION_DAYS)
summary_df = summary_df[summary_df["last_date"] >= valid_cutoff]


summary_df = summary_df.sort_values(by=["last_signal", "signal_score"], ascending=[True, False])
summary_df.to_csv(SUMMARY_CSV, index=False)

# Save to DB
with sqlite3.connect(DB_PATH) as conn:
    summary_df.to_sql("institutional_watch", conn, if_exists="replace", index=False)
    history_df.to_sql("institutional_watch_history", conn, if_exists="replace", index=False)

print(f"✅ Saved {len(summary_df)} summary entries and {len(history_df)} history rows.")
