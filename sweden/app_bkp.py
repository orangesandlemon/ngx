# === File: app.py ===
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from datetime import datetime

DB_PATH = "data/omx_equities.db"
SUMMARY_CSV = "institutional_watch_se.csv"
HISTORY_CSV = "institutional_watch_history.csv"

st.set_page_config(page_title="🇸🇪 Institutional Watch - Sweden", layout="wide")
st.title(":flag-se: Institutional Watch Dashboard")

# === Load Data ===
try:
    summary_df = pd.read_csv(SUMMARY_CSV)
    history_df = pd.read_csv(HISTORY_CSV)
    conn = sqlite3.connect(DB_PATH)
    equity_df = pd.read_sql("SELECT * FROM equities", conn)
    conn.close()
except FileNotFoundError:
    st.error("❌ Required files not found. Please run the scripts first.")
    st.stop()

if summary_df.empty or history_df.empty:
    st.warning("⚠️ No institutional signals found.")
    st.stop()

# === Sidebar Filters ===
st.sidebar.header("🔍 Filter")
selected_stock = st.sidebar.selectbox("Select a stock", ["All"] + sorted(summary_df["name"].unique()))
tier_filter = st.sidebar.multiselect("Tier", ["watchlist", "buildup", "buy", "sell"], default=["watchlist", "buildup", "buy"])

# === Summary View ===
st.subheader("📊 Summary View")
filtered_summary = summary_df[summary_df["tier"].isin(tier_filter)]
if selected_stock != "All":
    filtered_summary = filtered_summary[filtered_summary["name"] == selected_stock]
st.dataframe(filtered_summary, use_container_width=True)

st.subheader("📈 Dry Accumulation")
#if selected_stock != "All":
equity_df["date"] = pd.to_datetime(equity_df["date"])
equity_df = equity_df.sort_values(by=["name", "date"])

if selected_stock != "All":
    equity_df = equity_df[equity_df["name"] == selected_stock]

patterns = []
for name, group in equity_df.groupby("name"):
    group = group.reset_index(drop=True)
    for i in range(2, len(group)):
        day1, day2, day3 = group.loc[i-2], group.loc[i-1], group.loc[i]
        day2_up_lowvol = day2["close"] > day1["close"] and day2["volume"] < day1["volume"]
        if day2_up_lowvol and (
            (day3["close"] <= day2["close"] and day3["volume"] > day2["volume"]) or
            (day3["close"] > day2["close"] and day3["volume"] < day2["volume"])
        ):
            # Check if day4 exists
            if i + 1 < len(group):
                day4 = group.loc[i+1]
                if day4["close"] > day3["close"] and day4["volume"] < day3["volume"]:
                    patterns.append({
                        "name": name,
                        "watch": day2["date"],
                        "buildup": day3["date"],
                        "buy": day4["date"],
                        "buy_price": day4["close"],
                        "marketcap": int(day2["market_cap"]) if not pd.isna(day2["market_cap"]) else None
                    })

pattern_df = pd.DataFrame(patterns)
if pattern_df.empty:
    st.info("No dry accumulation pattern found in last 90 days.")
else:
    st.success(f"✅ {len(pattern_df)} dry accumulation patterns found.")
    st.dataframe(pattern_df, use_container_width=True)


# === Signal Timeline ===
st.subheader("📈 Signal Progression")
if selected_stock == "All":
    st.info("Select a specific stock to view its signal progression.")
else:
    stock_history = history_df[history_df["name"] == selected_stock]
    if stock_history.empty:
        st.warning("No history found for this stock.")
    else:
        latest_signal = stock_history.iloc[-1]
        st.markdown(f"### 🧠 Latest Signal for **`{selected_stock}`**: **{latest_signal['signal']}** | Score: **{latest_signal['signal_score']}**")
        st.caption(f"Reason: {latest_signal['action_reason']}")

        tab1, tab2, tab3 = st.tabs(["Signal Timeline", "Volume Timeline", "Dry Accumulation Alerts"])

        with tab1:
            fig = px.bar(
                stock_history,
                x="date",
                y="signal_score",
                color="signal",
                hover_data=["volume", "action_reason"],
                color_discrete_map={
                    "watchlist": "gray",
                    "buildup": "orange",
                    "buy_small": "deepskyblue",
                    "buy_confirmed": "green",
                    "buy": "green",
                    "sell": "red"
                },
                labels={"signal_score": "Signal Score", "date": "Date"},
                title="Signal Score Progression"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with tab2:
            fig2 = px.bar(
                stock_history,
                x="date",
                y="volume",
                color="signal",
                hover_data=["signal_score", "action_reason"],
                color_discrete_map={
                    "watchlist": "gray",
                    "buildup": "orange",
                    "buy_small": "deepskyblue",
                    "buy_confirmed": "green",
                    "buy": "green",
                    "sell": "red"
                },
                labels={"volume": "Volume", "date": "Date"},
                title="Volume per Stealth Day"
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            equity_df["date"] = pd.to_datetime(equity_df["date"])
            equity_df = equity_df.sort_values(by=["name", "date"])

            patterns = []
board = {}

for name, group in equity_df.groupby("name"):
    group = group.reset_index(drop=True)
    for i in range(2, len(group)):
        day1, day2, day3 = group.loc[i-2], group.loc[i-1], group.loc[i]
        key = f"{name}_{day2['date']}"

        # === Day 2 signal
        day2_up_lowvol = day2["close"] > day1["close"] and day2["volume"] < day1["volume"]

        # === Day 3 validation
        valid_day3 = (
            day2_up_lowvol and (
                (day3["close"] <= day2["close"] and day3["volume"] > day2["volume"]) or
                (day3["close"] > day2["close"] and day3["volume"] < day2["volume"])
            )
        )

        if valid_day3:
            # Save Day 2 and Day 3 — "setup forming"
            board[key] = {
                "name": name,
                "watch": day2["date"],
                "buildup": day3["date"],
                "buy": None,
                "buy_price": None,
                "marketcap": int(day2["market_cap"]) if not pd.isna(day2["market_cap"]) else None,
                "pending_day4_index": i + 1
            }

        # === Check Day 4 (if pattern in board)
        if key in board and i == board[key]["pending_day4_index"]:
            day4 = day3  # now `day3` is actually Day 4 in loop
            day3 = group.loc[i-1]
            # Check Day 4 condition
            if day4["close"] > day3["close"] and day4["volume"] < day3["volume"]:
                board[key]["buy"] = day4["date"]
                board[key]["buy_price"] = day4["close"]
            else:
                # Invalidate pattern if Day 4 fails
                del board[key]

# Finalize patterns (only those with buy dates or ongoing setups)
for pattern in board.values():
    if pattern["buy"]:  # completed
        patterns.append(pattern)
    else:
        patterns.append(pattern)  # optional: show pending setups

# Create DataFrame
pattern_df = pd.DataFrame(patterns)
if pattern_df.empty:
    st.info("No dry accumulation pattern found in last 90 days.")
else:
    st.success(f"✅ {len(pattern_df)} dry accumulation patterns found.")
    st.dataframe(pattern_df, use_container_width=True)


st.markdown("---")
st.markdown("❔ **How to Interpret:**")
st.markdown("""
- **watchlist**: Volume spike with small price gain — early stealth accumulation
- **buildup**: At least 2 stealth days in a row
- **buy**: Confirmed breakout
- **sell**: Price drop after buy — possible exit
""")

st.caption("Built using 3-month data. Track progression from Watch ➝ Buildup ➝ Buy ➝ Sell like a pro.")
