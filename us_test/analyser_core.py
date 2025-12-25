def run_analyzer_on_dataframe(df, db_path, skip_summary=False):
    from analyser_us_test import smart_score, format_reason_text
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(db_path)
    memory_cursor = conn.cursor()
    signals = []

    # === Rolling Features ===
    df = df.sort_values(by=["name", "date"]).copy()
    df["volume_5_avg"] = df.groupby("name")["volume"].rolling(5, min_periods=1).mean().reset_index(level=0, drop=True).round(2)
  
    df["price_5_high"] = df.groupby("name")["high"].rolling(5, min_periods=1).max().reset_index(level=0, drop=True).round(2)
    df["low_5"] = df.groupby("name")["low"].rolling(5, min_periods=1).min().reset_index(level=0, drop=True).round(2)

    df["volume_15_avg"] = df.groupby("name")["volume"].rolling(15, min_periods=5).mean().reset_index(level=0, drop=True).round(2)
    df["volume_uptrend"] = df.groupby("name")["volume_15_avg"].transform(lambda x: x > x.shift(5))
    df["inst_footprint"] = (df["volume"] > df["volume_5_avg"]) 
    df["inst_accum_30"] = df.groupby("name")["inst_footprint"].rolling(30, min_periods=10).sum().reset_index(level=0, drop=True).round(0)

    df["price_change_15"] = df.groupby("name")["close"].pct_change(15).round(4)
    df["volume_change_15"] = df.groupby("name")["volume"].pct_change(15).round(4)
    df["stealth_accum"] = (df["volume_change_15"] > 0.3) & (df["price_change_15"] < 0.05)

    df["prev_high"] = df.groupby("name")["high"].shift(1)
    df["prev_low"] = df.groupby("name")["low"].shift(1)
    df["higher_high"] = df["high"] > df["prev_high"]
    df["higher_low"] = df["low"] > df["prev_low"]

    df["previous_close"] = df.groupby("name")["close"].shift(1)
    df["gap_up"] = df["open"] > df["previous_close"] * 1.02
    df["gap_down"] = df["open"] < df["previous_close"] * 0.98

    
    for _, row in df.iterrows():
        if pd.isna(row["volume_5_avg"]):
            ptint(row)
            continue

        score, reasons, signal, action = smart_score(row)
        print("🔎 Sample row input to smart_score:")
        print(row)


        # === Scoring Enhancements ===
        if row.get("volume_uptrend", False):
            score += 10
            reasons.append("15-Day Volume Uptrend")
        if row.get("inst_accum_30", 0) >= 10:
            score += 15
            reasons.append("10 of 30 Days Institutional Pattern")
        if row.get("stealth_accum", False):
            score += 20
            reasons.append("Stealth Accumulation")
        if row["volume"] > 1.2 * row["volume_5_avg"]:
            score += 10
            reasons.append("Volume spike vs 5-day avg")
        if row["volume"] > row["volume_15_avg"]:
            score += 5
            reasons.append("Above 15-day average volume")
        if row["high"] >= 0.98 * row["price_5_high"]:
            score += 7
            reasons.append("Near recent 5-day high")

        # === Limit-Up Logic ===
        streak = int(row.get("limit_up_streak", 0))
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

        # === Memory Logic ===
        memory_row = pd.read_sql_query(
            "SELECT * FROM signal_memory WHERE name = ? ORDER BY date DESC LIMIT 1",
            conn, params=(row["name"],)
        )

        last_close = memory_row["last_close"].iloc[0] if not memory_row.empty else None
        last_action = memory_row["last_action"].iloc[0] if not memory_row.empty else None

        if last_close and abs(row["close"] - last_close) <= 0.02 * last_close:
            score += 5
            reasons.append("Retesting key level")

        if last_action == "BUY" and signal == "WATCH":
            reasons.append("Signal weakening")
            signal = "EXIT"
            action = "EXIT"

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
                
                "value": row["value"],
                "close": row["close"],
                "change": row["change"],
                "action": action,
                "buy_range": buy_range,
                "explanation": format_reason_text(reasons, row),
                "limit_up_streak": streak,
                "signal_tier": "confirmed" if score >= 75 else "setup" if score >= 60 else "watchlist" if score >= 40 else "none",
                "volume_uptrend": row.get("volume_uptrend", False),
                "inst_accum_30": row.get("inst_accum_30", 0),
                "stealth_accum": row.get("stealth_accum", False)
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

    # === 💾 Optional summary writing ===
    if not skip_summary:
        from analyser_us_test import save_daily_summary, generate_summary_text
        try:
            summary_text, action_notes = generate_summary_text(signals)
            save_daily_summary(summary_text, action_notes, conn)
        except Exception as e:
            print("⚠️ Skipping summary write due to error:", e)

    conn.commit()
    conn.close()
    return signals
