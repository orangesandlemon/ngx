# analyser.py
import sqlite3
import pandas as pd
from datetime import datetime
import smtplib
import os
from dotenv import load_dotenv

# Skip if it's Saturday (5) or Sunday (6)
if datetime.today().weekday() >= 5:
    print("🛑 Market closed (weekend). Exiting.")
    exit()

DB_PATH = "data/ngx_equities.db"

# Load data
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by=["name", "date"])

signals = []

# Group by stock
for name, group in df.groupby("name"):
    group = group.sort_values("date")
    group["avg_volume_5"] = group["volume"].rolling(5).mean()
    group["avg_trades_5"] = group["trades"].rolling(5).mean()

    # Calculate limit-up streak
    group["limit_up"] = (
        (group["close"] - group["previous_close"]) / group["previous_close"]
    ).round(4) >= 0.099

    group["limit_up_streak"] = group["limit_up"].astype(int)
    group["limit_up_streak"] = group["limit_up_streak"].groupby(
        (group["limit_up_streak"] != group["limit_up_streak"].shift()).cumsum()
    ).cumsum()
    group.loc[~group["limit_up"], "limit_up_streak"] = 0

    for i in range(len(group)):
        row = group.iloc[i]
        if pd.isna(row["avg_volume_5"]):
            continue

        signal = None
        action = None
        buy_range = None
        score = 0
        reasons = []

        streak = int(row["limit_up_streak"])

        # 🎯 Limit-Up Streak Logic
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

            low_price = min(row["open"], row["close"])
            high_price = max(row["open"], row["close"])
            buy_range = f"₦{low_price:.2f} – ₦{high_price:.2f}"

        # 💥 Moon Alert
        elif (
            row["close"] > row["open"]
            and row["volume"] > 2 * row["avg_volume_5"]
            and row["trades"] < row["avg_trades_5"]
        ):
            signal = "💥 Institution Moon Alert"
            action = "BUY"
            score += 40
            reasons += ["Limit-Up", "Moon Volume", "Low Trades - likely institutional"]
            low_price = min(row["open"], row["close"])
            high_price = max(row["open"], row["close"])
            buy_range = f"₦{low_price:.2f} – ₦{high_price:.2f}"

        # 📈 Regular Signals
        else:
            if row["close"] > row["open"]:
                score += 20
                reasons.append("Price Up")
            if row["volume"] > 1.5 * row["avg_volume_5"]:
                score += 30
                reasons.append("Volume Spike")
            if row["trades"] < row["avg_trades_5"]:
                score += 30
                reasons.append("Low Trade Count")
            if row["value"] > 50_000_000:
                score += 20
                reasons.append("High Value")
            if row["trades"] > 2 * row["avg_trades_5"]:
                score += 30
                reasons.append("High Trade Count")
            if row["close"] < row["open"]:
                score += 20
                reasons.append("Price Down")

            if "Price Up" in reasons and "Volume Spike" in reasons and "Low Trade Count" in reasons:
                signal = "Institutional Accumulation"
                action = "BUY"
            elif "Price Up" in reasons and "High Trade Count" in reasons:
                signal = "Retail Buying Frenzy"
                action = "AVOID"
            elif "Price Down" in reasons and "Volume Spike" in reasons and "High Trade Count" in reasons:
                signal = "Distribution Exit"
                action = "SELL / AVOID"

            if signal in ["Institutional Accumulation", "Retail Buying Frenzy", "Distribution Exit"]:
                if action == "BUY":
                    low_price = min(row["open"], row["close"])
                    high_price = max(row["open"], row["close"])
                    buy_range = f"₦{low_price:.2f} – ₦{high_price:.2f}"
                else:
                    buy_range = "—"

        if signal:
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
                "buy_range": buy_range,
                "explanation": ", ".join(reasons),
                "limit_up_streak": streak
            })

# ✅ Store results
signals_df = pd.DataFrame(signals)

if not signals_df.empty:
    signals_df.to_sql("signals", conn, if_exists="replace", index=False)
    print(f"✅ {len(signals_df)} signals stored in 'signals' table.")
    print(signals_df.head(3))
else:
    print("⚠️ No signals found (need more days of data).")

conn.close()

# === EMAIL SECTION ===
def send_email(subject, body):
    load_dotenv()
    EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    TO_EMAIL = os.getenv("TO_EMAIL")

    message = f"Subject: {subject}\n\n{body}"

    try:
        with smtplib.SMTP("smtp.mail.yahoo.com", 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, TO_EMAIL, message)
        print("📬 Email alert sent.")
    except Exception as e:
        print(f"❌ Email sending failed: {e}")

# Send daily alerts if signals found
if not signals_df.empty:
    summary_lines = []
    for _, row in signals_df.iterrows():
        summary_lines.append(
            f"📌 {row['name']} - {row['signal']} ({row['date']})\n"
            f"Action: {row['action']} | Buy Range: {row['buy_range']}\n"
            f"Confidence: {row['confidence_score']} | Reason: {row['explanation']}\n"
            "—"
        )

    email_body = "\n\n".join(summary_lines)
    send_email("📈 NGX Daily Signals", email_body)
