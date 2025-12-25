# === File: app.py ===
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from datetime import datetime
import os

DB_PATH = "data/ngx_equities.db"
SUMMARY_CSV = "institutional_watch_se.csv"
HISTORY_CSV = "institutional_watch_history.csv"
FOCUS_CSV = "focus_list.csv"

st.set_page_config(page_title="🇳🇬 Institutional Watch - Naija_test", layout="wide")
st.title("📈 Institutional Watch Dashboard")

# === Initialize Focus List CSV if not exists ===
if not os.path.exists(FOCUS_CSV):
    pd.DataFrame(columns=["name", "comment", "date_added"]).to_csv(
        FOCUS_CSV, index=False
    )

focus_df = pd.read_csv(FOCUS_CSV)

# === Load Data ===
try:
    summary_df = pd.read_csv(SUMMARY_CSV)
    history_df = pd.read_csv(HISTORY_CSV)
    conn = sqlite3.connect(DB_PATH)
    equity_df = pd.read_sql(
        "SELECT name, date, close, volume, trades FROM equities", conn
    )
    conn.close()
except FileNotFoundError:
    st.error("❌ Required files not found. Please run the scripts first.")
    st.stop()

if summary_df.empty or history_df.empty:
    st.warning("⚠️ No institutional signals found.")
    st.stop()

# === Sidebar Filters ===
st.sidebar.header("🔍 Filter")
selected_stock = st.sidebar.selectbox(
    "Select a stock", ["All"] + sorted(summary_df["name"].unique())
)
tier_filter = st.sidebar.multiselect(
    "Tier",
    ["watchlist", "buildup", "🚨 buy_zone", "breakout"],
    default=["watchlist", "buildup", "🚨 buy_zone"],
)

# === Equity Data Preparation ===
equity_df["date"] = pd.to_datetime(equity_df["date"])
equity_df = equity_df.sort_values(by=["name", "date"])
if selected_stock != "All":
    equity_df = equity_df[equity_df["name"] == selected_stock]


# === Pattern Detection ===
patterns = []
for name, group in equity_df.groupby("name"):
    group = group.reset_index(drop=True)
    group["volume_avg_10"] = group["volume"].rolling(window=10).mean()
    group["volume_avg_30"] = group["volume"].rolling(window=30).mean()

    tier_order = {"watchlist": 1, "buildup": 2, "🚨 buy_zone": 3, "breakout": 4}
    last_tier = None
    last_tier_price = None

    for i in range(30, len(group)):
        price = group.loc[i, "close"]
        vol = group.loc[i, "volume"]
        trades = group.loc[i, "trades"]
        vol_avg = group["volume"].iloc[i - 5 : i].mean()
        trades_avg = group["trades"].iloc[i - 5 : i].mean()

        # Reset logic if price drops significantly
        if last_tier_price and price < last_tier_price * 0.92:
            last_tier = None
            last_tier_price = None

        price_std = group["close"].iloc[i - 10 : i].std()
        price_range_pct = price / group.loc[i - 5, "close"]

        watchlist = (
            (price_std < 0.5 or 0.96 <= price_range_pct <= 1.05)
            and vol <= group["volume"].iloc[i - 5 : i].mean() * 1.3
            and trades >= group["trades"].iloc[i - 5 : i].mean() * 0.7
        )

        rising_days = sum(
            [
                group.loc[k, "close"] > group.loc[k - 1, "close"]
                for k in range(i - 4, i + 1)
            ]
        )
        price_up_total = sum(
            [
                group.loc[k, "close"] - group.loc[k - 1, "close"]
                for k in range(i - 4, i + 1)
                if group.loc[k, "close"] > group.loc[k - 1, "close"]
            ]
        )

        steady_rise = (
            rising_days >= 3
            and price_up_total / group.loc[i - 5, "close"] < 0.15
            and trades > trades_avg
            and vol > vol_avg
        )

        volume_cluster = (
            vol > group["volume_avg_30"].iloc[i] * 2
            and abs(price - group.loc[i - 1, "close"]) < 1.0
        )

        accum_days = sum(
            [
                abs(group.loc[j, "close"] - group.loc[j - 1, "close"]) < 1.0
                and group.loc[j, "volume"] > group["volume"].iloc[j - 5 : j].mean()
                and group.loc[j, "trades"] < group["trades"].iloc[j - 5 : j].mean()
                for j in range(i - 5, i)
                if j > 0
            ]
        )
        multi_day_buildup = accum_days >= 3

        buildup = steady_rise or volume_cluster or multi_day_buildup
        price_ref = group.iloc[i - 5]["close"] if i >= 5 else None
        buy_setup_zone = (
            rising_days >= 3
            and 1.05 < price / price_ref
            and vol > vol_avg * 1.1
            and trades >= trades_avg * 0.9
        )

        stealth = (
            group.loc[i - 1, "close"] > group.loc[i - 2, "close"]
            and group.loc[i - 1, "volume"] > group.loc[i - 2, "volume"] * 1.2
            and group.loc[i - 1, "trades"] < group.loc[i - 2, "trades"] * 0.95
            and group.loc[i - 1, "volume"] > group["volume_avg_30"].iloc[i] * 2
        )

        buy_zone = stealth or buy_setup_zone

        breakout = price > group["close"].iloc[i - 10 : i].max() and vol > vol_avg * 1.5

        current_tier = None
        if breakout:
            current_tier = "breakout"
        elif buy_zone:
            current_tier = "🚨 buy_zone"
        elif buildup:
            current_tier = "buildup"
        elif watchlist:
            current_tier = "watchlist"

        if current_tier and tier_order.get(current_tier, 0) > tier_order.get(
            last_tier, 0
        ):
            patterns.append(
                {
                    "name": name,
                    "signal": current_tier,
                    "date": group.loc[i, "date"],
                    "price": price,
                    "volume": vol,
                    "trades": trades,
                }
            )
            last_tier = current_tier
            last_tier_price = price

patterns = pd.DataFrame(patterns)


# === Silent Pressure Detection ===
silent_df = equity_df.copy()
silent_df = silent_df.sort_values(by=["name", "date"])
silent_df["prev_close"] = silent_df.groupby("name")["close"].shift(1)
silent_df["price_change"] = silent_df["close"] - silent_df["prev_close"]

# Detect 5-day flat close followed by price drop
silent_df["flat"] = silent_df.groupby("name")["close"].transform(
    lambda x: x.rolling(5).apply(lambda y: pd.Series(y).nunique() == 1, raw=False)
)
silent_df["drop_after_flat"] = (silent_df["flat"] == 1.0) & (
    silent_df["price_change"] < -0.5
)

# Extract and label
silent_signals = silent_df[silent_df["drop_after_flat"]].copy()
silent_signals["signal"] = "🧨 silent_pressure"
silent_signals = silent_signals[["name", "date", "close", "signal"]].rename(
    columns={"close": "price"}
)

# Append to patterns
patterns = pd.concat([patterns, silent_signals], ignore_index=True)


# === Focus List Section ===
st.subheader("📌 My Focus List")
with st.expander("➕ Add to Focus List"):
    add_name = st.selectbox("Select stock to add", sorted(equity_df["name"].unique()))
    add_comment = st.text_input("Comment")
    if st.button("Add to Focus List"):
        if add_name not in focus_df["name"].values:
            new_entry = pd.DataFrame(
                {
                    "name": [add_name],
                    "comment": [add_comment],
                    "date_added": [datetime.today().strftime("%Y-%m-%d")],
                }
            )
            focus_df = pd.concat([focus_df, new_entry], ignore_index=True)
            focus_df.to_csv(FOCUS_CSV, index=False)
            st.rerun()

# === Get latest signal per stock from full pattern set ===
latest_patterns = patterns.sort_values("date").drop_duplicates("name", keep="last")

# === Merge with focus list (manual selections) ===
focus_merged = focus_df.merge(latest_patterns, on="name", how="left")

# === Optional tier sorting for better UX ===
tier_order = {"watchlist": 1, "buildup": 2, "🚨 buy_zone": 3, "breakout": 4}
focus_merged["tier_rank"] = focus_merged["signal"].map(tier_order)
focus_merged = focus_merged.sort_values(by="tier_rank", ascending=True)

# === Display or empty state ===
if not focus_merged.empty:
    st.markdown("### ⭐ Focus List (Your Manual Watch Selections)")
    st.dataframe(
        focus_merged[
            ["name", "signal", "date", "price", "volume", "trades", "comment"]
        ],
        use_container_width=True,
    )

    remove_stock = st.selectbox(
        "Remove from Focus List", ["None"] + sorted(focus_df["name"].tolist())
    )
    if remove_stock != "None" and st.button("Remove"):
        focus_df = focus_df[focus_df["name"] != remove_stock]
        focus_df.to_csv(FOCUS_CSV, index=False)
        st.rerun()
else:
    st.info("No stocks in your Focus List yet.")


# === Load signals ===
@st.cache_data
def load_signals():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM signals ORDER BY date DESC", conn)
    conn.close()
    return df


df = load_signals()


def highlight_row(row):
    if "Limit-Up" in row["signal"] or row["action"] in ["BUY CONFIRMED", "BUY SMALL"]:
        return ["background-color: #fff7e6"] * len(row)  # Light gold
    else:
        return [""] * len(row)


df = load_signals()

page = st.radio(
    "Choose a section:",
    [
        "🔍 Signals",
        "📈 Price Change Patterns",
        "🕵️ Institutional Watchlist",
        "📊 Weekly Intelligence",
    ],
    horizontal=True,
)

if page == "🔍 Signals":
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        selected_signal = st.selectbox(
            "Filter by Signal Type", ["All"] + sorted(df["signal"].unique().tolist())
        )

    with col2:
        selected_action = st.selectbox(
            "Filter by Action", ["All"] + sorted(df["action"].unique().tolist())
        )

    with col3:
        selected_date = st.selectbox(
            "Filter by Date",
            ["All"] + sorted(df["date"].unique().tolist(), reverse=True),
        )

    with col4:
        selected_name = st.selectbox(
            "Filter by Name", ["All"] + sorted(df["name"].unique().tolist())
        )

    with col5:
        selected_confidence_score = st.selectbox(
            "Filter by Confidence Score",
            ["All"] + sorted(df["confidence_score"].unique().tolist(), reverse=True),
        )

    # === Apply filters ===
    filtered_df = df.copy()

    if selected_signal != "All":
        filtered_df = filtered_df[filtered_df["signal"] == selected_signal]

    if selected_action != "All":
        filtered_df = filtered_df[filtered_df["action"] == selected_action]

    if selected_date != "All":
        filtered_df = filtered_df[filtered_df["date"] == selected_date]

    if selected_name != "All":
        filtered_df = filtered_df[filtered_df["name"] == selected_name]

    if selected_confidence_score != "All":
        filtered_df = filtered_df[
            filtered_df["confidence_score"] == selected_confidence_score
        ]

    # === Display ===
    st.subheader(f"📊 Showing {len(filtered_df)} signals")

    if st.button("🔄 Refresh Signals"):
        st.cache_data.clear()
        st.rerun()

    st.dataframe(
        filtered_df[
            [
                "name",
                "date",
                "signal",
                "confidence_score",
                "action",
                "buy_range",
                "explanation",
                "signal_tier",
            ]
        ].style.apply(highlight_row, axis=1),
        use_container_width=True,
    )

elif page == "📈 Price Change Patterns":
    st.subheader("📈 Price Change (%) Over Time")

    conn = sqlite3.connect(DB_PATH)
    df_pct = pd.read_sql(
        "SELECT name, date, change_pct, close FROM equities where volume > 0 AND date > '2025-06-01' ORDER BY date DESC",
        conn,
    )
    conn.close()

    # Format and clean
    df_pct["date"] = pd.to_datetime(df_pct["date"])
    # Remove bad values
    df_pct = df_pct.dropna(subset=["change_pct"])
    df_pct = df_pct[~df_pct["change_pct"].isin([float("inf"), float("-inf")])]
    df_pct = df_pct[df_pct["change_pct"] != 0]

    stock_names = sorted(df_pct["name"].unique())
    selected_name = st.selectbox("🔎 Choose a stock", stock_names)

    filtered_df = df_pct[df_pct["name"] == selected_name].sort_values("date")

    # Date slider to narrow view
    from datetime import datetime

    min_date = pd.to_datetime(df["date"].min()).to_pydatetime()
    max_date = pd.to_datetime(df["date"].max()).to_pydatetime()

    start_date, end_date = st.slider(
        "🗓 Select date range to zoom in",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
    )

    zoomed_df = filtered_df[
        (filtered_df["date"] >= start_date) & (filtered_df["date"] <= end_date)
    ]

    st.markdown("### 🔍 Zoomed-In View")
    st.line_chart(zoomed_df.set_index("date")["change_pct"])

    # ✅ Dual-axis chart: change_pct (left), price (right)
    import matplotlib.pyplot as plt

    fig, ax1 = plt.subplots(figsize=(12, 4))

    ax1.plot(zoomed_df["date"], zoomed_df["change_pct"], color="blue", label="% Change")
    ax1.set_ylabel("Change (%)", color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")
    ax1.axhline(0, linestyle="--", color="gray", linewidth=0.7)

    ax2 = ax1.twinx()
    ax2.plot(zoomed_df["date"], zoomed_df["close"], color="orange", label="Close Price")
    ax2.set_ylabel("Price (₦)", color="orange")
    ax2.tick_params(axis="y", labelcolor="orange")

    fig.autofmt_xdate()
    st.pyplot(fig)

    st.markdown("## 🧭 Quick View: All Stock Graphs")

    # Show all stocks in compact form
    cols = st.columns(2)  # Two columns per row
    col_idx = 0

    for stock in stock_names:
        stock_df = df_pct[df_pct["name"] == stock].sort_values("date")

        if len(stock_df) < 5:
            continue  # skip very small series

        fig, ax1 = plt.subplots(figsize=(5, 2))  # small chart

        ax1.plot(stock_df["date"], stock_df["change_pct"], color="blue")
        ax1.set_ylabel("%", color="blue")
        ax1.tick_params(axis="y", labelcolor="blue")
        ax1.axhline(0, linestyle="--", color="gray", linewidth=0.5)

        ax2 = ax1.twinx()
        ax2.plot(stock_df["date"], stock_df["close"], color="orange")
        ax2.set_ylabel("₦", color="orange")
        ax2.tick_params(axis="y", labelcolor="orange")

        ax1.set_title(stock, fontsize=10)
        ax1.set_xticks([])  # hide x-axis ticks
        ax2.set_xticks([])

        fig.tight_layout()

        # Show in alternating columns
        with cols[col_idx]:
            st.pyplot(fig)

        col_idx = (col_idx + 1) % 2  # Switch column


elif page == "📊 Weekly Intelligence":
    st.subheader("📊 Weekly Trade Intelligence (Last 4 Days)")

    conn = sqlite3.connect(DB_PATH)
    df_intel = pd.read_sql(
        """
        SELECT * FROM weekly_intel
        WHERE score = 3
        ORDER BY name ASC
    """,
        conn,
    )

    conn.close()

    if df_intel.empty:
        st.info("No weekly intelligence data available. Run weekly_intel.py first.")
    else:
        st.caption(
            "🚨 Highlighting weekly trade and volume anomalies to help track persistent activity."
        )

    st.dataframe(
        df_intel[
            [
                "name",
                "trades_0",
                "trades_1",
                "trade_spike",
                "volume_0",
                "volume_1",
                "volume_spike",
                "avg_change_1",
                "stealth_accum_candidate",
                "momentum_spike",
                "score",
                "trend_tag",
                "date_generated",
            ]
        ],
        use_container_width=True,
    )

elif page == "🕵️ Institutional Watchlist":
    st.header("🕵️ Institutional Accumulation Watch")

    conn = sqlite3.connect(DB_PATH)
    df_inst = pd.read_sql(
        "SELECT * FROM institutional_watch ORDER BY stealth_days DESC", conn
    )

    st.dataframe(
        df_inst[
            [
                "name",
                "stealth_days",
                "avg_volume_14",
                "avg_change_14",
                "last_close",
                "zone",
                "date_generated",
            ]
        ],
        use_container_width=True,
    )
# === Interpretation ===
st.markdown("---")
st.markdown("❔ **How to Interpret Levels:**")
st.markdown(
    """
🟡 Watchlist → Quiet accumulation phase.
Price is stable, volume is consistent, and trades are holding steady. Institutions may be positioning slowly. Monitor closely — this is your early radar.

🟠 Buildup → Smart money is moving in.
Volume and trade activity are rising, often without a major price jump. This is the ideal entry point. Buy here to position early before a breakout.

🔴 🚨 Buy Zone → Breakout is imminent.
Price has climbed 5–10% with strong volume and trade confirmation. If seen, it confirms momentum. You can add or enter boldly if you missed the buildup.

✅ Breakout → Price and volume surge.
Market has noticed. You're now in the public phase. Be cautious — it's often too late to enter, but useful for tracking when to hold, trail-stop, or sell partials.
"""
)
st.caption("Built to track stealth ➝ buildup ➝ buy zone ➝ breakout like a pro.")
