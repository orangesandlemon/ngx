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
selected_stock = st.sidebar.selectbox(
    "Select a stock", ["All"] + sorted(summary_df["name"].unique())
)
tier_filter = st.sidebar.multiselect(
    "Tier",
    ["watchlist", "buildup", "buy", "sell"],
    default=["watchlist", "buildup", "buy"],
)

# === Summary View ===
st.subheader("📊 Summary View")
filtered_summary = summary_df[summary_df["tier"].isin(tier_filter)]
if selected_stock != "All":
    filtered_summary = filtered_summary[filtered_summary["name"] == selected_stock]
st.dataframe(filtered_summary, use_container_width=True)


st.subheader("📈 Dry Accumulation")
# if selected_stock != "All":
equity_df["date"] = pd.to_datetime(equity_df["date"])
equity_df = equity_df.sort_values(by=["name", "date"])

if selected_stock != "All":
    equity_df = equity_df[equity_df["name"] == selected_stock]

patterns = []
for name, group in equity_df.groupby("name"):
    group = group.reset_index(drop=True)
    for i in range(2, len(group)):
        day1, day2, day3 = group.loc[i - 2], group.loc[i - 1], group.loc[i]
        day2_up_lowvol = (
            day2["close"] > day1["close"] and day2["volume"] < day1["volume"]
        )
        if day2_up_lowvol and (
            (day3["close"] <= day2["close"] and day3["volume"] > day2["volume"])  # or
            # (day3["close"] > day2["close"] and day3["volume"] < day2["volume"])
        ):
            # Check if day4 exists
            if i + 1 < len(group):
                day4 = group.loc[i + 1]
                if day4["close"] >= day3["close"] and day4["volume"] < day3["volume"]:
                    patterns.append(
                        {
                            "name": name,
                            "watch": day2["date"],
                            "buildup": day3["date"],
                            "buy": day4["date"],
                            "buy_price": day4["close"],
                            "marketcap": (
                                int(day2["market_cap"])
                                if not pd.isna(day2["market_cap"])
                                else None
                            ),
                        }
                    )

pattern_df = pd.DataFrame(patterns)
if pattern_df.empty:
    st.info("No dry accumulation pattern found in last 90 days.")
else:
    st.success(f"✅ {len(pattern_df)} dry accumulation patterns found.")
    st.dataframe(pattern_df, use_container_width=True)


st.subheader("📈 Dry Accumulation (Progressive Tracker)")

equity_df["date"] = pd.to_datetime(equity_df["date"])
equity_df = equity_df.sort_values(by=["name", "date"])

if selected_stock != "All":
    equity_df = equity_df[equity_df["name"] == selected_stock]

pattern_tracker = []

for name, group in equity_df.groupby("name"):
    group = group.reset_index(drop=True)
    for i in range(2, len(group)):
        day1 = group.loc[i - 2]
        day2 = group.loc[i - 1]
        day3 = group.loc[i]
        record = {
            "name": name,
            "watch_date": None,
            "buildup_date": None,
            "buy_date": None,
            "buy_price": None,
            "marketcap": (
                int(day2["market_cap"]) if not pd.isna(day2["market_cap"]) else None
            ),
        }

        # Day 2 logic (Watch condition)
        day2_up_lowvol = (
            day2["close"] > day1["close"] and day2["volume"] < day1["volume"]
        )
        if day2_up_lowvol:
            record["watch_date"] = day2["date"]

            # Day 3 logic (Buildup condition)
            if day3["close"] <= day2["close"] and day3["volume"] > day2["volume"]:
                record["buildup_date"] = day3["date"]

                # Optional Day 4 (Buy condition)
                if i + 1 < len(group):
                    day4 = group.loc[i + 1]
                    if (
                        day4["close"] >= day3["close"]
                        and day4["volume"] < day3["volume"]
                    ):
                        record["buy_date"] = day4["date"]
                        record["buy_price"] = day4["close"]

            pattern_tracker.append(record)

# Filter to show only useful signals
pattern_df = pd.DataFrame(pattern_tracker)
pattern_df = pattern_df.dropna(
    subset=["watch_date"]
)  # only show if at least Watch is set

if pattern_df.empty:
    st.info("No dry accumulation in progress.")
else:
    st.success(f"📊 {len(pattern_df)} developing accumulation patterns.")
    st.dataframe(
        pattern_df.sort_values("watch_date", ascending=False), use_container_width=True
    )


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
        st.markdown(
            f"### 🧠 Latest Signal for **`{selected_stock}`**: **{latest_signal['signal']}** | Score: **{latest_signal['signal_score']}**"
        )
        st.caption(f"Reason: {latest_signal['action_reason']}")

        tab1, tab2, tab3 = st.tabs(
            ["Signal Timeline", "Volume Timeline", "Dry Accumulation Alerts"]
        )

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
                    "sell": "red",
                },
                labels={"signal_score": "Signal Score", "date": "Date"},
                title="Signal Score Progression",
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
                    "sell": "red",
                },
                labels={"volume": "Volume", "date": "Date"},
                title="Volume per Stealth Day",
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            equity_df["date"] = pd.to_datetime(equity_df["date"])
            equity_df = equity_df.sort_values(by=["name", "date"])

            patterns = []
            for name, group in equity_df.groupby("name"):
                group = group.reset_index(drop=True)
                for i in range(2, len(group)):
                    day1, day2, day3 = group.loc[i - 2], group.loc[i - 1], group.loc[i]
                    if (
                        day2["close"] > day1["close"]
                        and day2["volume"] < day1["volume"]
                        and day3["close"] <= day2["close"]
                        and day3["volume"] > day2["volume"]
                    ):
                        # Check if day4 exists
                        if i + 1 < len(group):
                            day4 = group.loc[i + 1]
                            if (
                                day4["close"] > day3["close"]
                                and day4["volume"] < day3["volume"]
                            ):
                                patterns.append(
                                    {
                                        "name": name,
                                        "watch": day2["date"],
                                        "buildup": day3["date"],
                                        "buy": day4["date"],
                                        "buy_price": day4["close"],
                                    }
                                )

            pattern_df = pd.DataFrame(patterns)
            if pattern_df.empty:
                st.info("No dry accumulation pattern found in last 90 days.")
            else:
                st.success(f"✅ {len(pattern_df)} dry accumulation patterns found.")
                st.dataframe(pattern_df, use_container_width=True)


# === Load signals ===
@st.cache_data
def load_signals():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM signals ORDER BY date DESC", conn)
    conn.close()
    return df


df = load_signals()


def human_readable(num):
    if pd.isna(num):
        return ""
    elif abs(num) >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f}B"
    elif abs(num) >= 1_000_000:
        return f"{num / 1_000_000:.2f}M"
    elif abs(num) >= 1_000:
        return f"{num / 1_000:.1f}K"
    else:
        return f"{num:.0f}"


def format_percent(x):
    return f"{x:.2f}%" if pd.notna(x) else ""


def highlight_row(row):
    if "Limit-Up" in row["signal"] or row["action"] in ["BUY CONFIRMED", "BUY SMALL"]:
        return ["background-color: #fff7e6"] * len(row)  # Light gold
    else:
        return [""] * len(row)


page = st.radio(
    "Choose a section:",
    [
        "🔍 Signals",
        "📊 Weekly Intelligence",
        "📘 Weekly Intelligence (10-Day)",
        "📊 Comparison Insights",
        "Match View Strong",
        "Match View Pull Back",
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
            ]
        ],
        use_container_width=True,
    )
elif page == "📊 Weekly Intelligence":
    st.subheader("📊 Weekly Trade Intelligence (Last 30 Days)")
    col1, col2, col3 = st.columns(3)

    conn = sqlite3.connect(DB_PATH)
    df_intel = pd.read_sql(
        """
        SELECT * FROM weekly_intel_100
        WHERE score >= 1 and avg_change_1 > avg_change_0 and avg_vol_1 > avg_vol_0 and close_end_1 > close_start_1
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

    with col1:
        selected_signal = st.selectbox(
            "Filter by Signal Type",
            ["All"] + sorted(df_intel["trend_tag"].unique().tolist()),
        )

    with col2:
        selected_date = st.selectbox(
            "Filter by Date",
            ["All"]
            + sorted(df_intel["date_generated"].unique().tolist(), reverse=True),
        )

    with col3:
        selected_name = st.selectbox(
            "Filter by Name", ["All"] + sorted(df_intel["name"].unique().tolist())
        )

    # === Apply filters ===
    filtered_df = df_intel.copy()

    if selected_signal != "All":
        filtered_df = filtered_df[filtered_df["trend_tag"] == selected_signal]

    if selected_date != "All":
        filtered_df = filtered_df[filtered_df["date_generated"] == selected_date]

    if selected_name != "All":
        filtered_df = filtered_df[filtered_df["name"] == selected_name]

    # Columns to format
    volume_cols = ["volume_0", "volume_1"]
    percent_cols = [
        "avg_change_0",
        "avg_change_1",
    ]

    for col in volume_cols:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].apply(human_readable)

    for col in percent_cols:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].apply(format_percent)

    st.dataframe(
        filtered_df[
            [
                "name",
                "volume_0",
                "volume_1",
                "volume_spike",
                "avg_change_0",
                "avg_change_1",
                "close_start_1",
                "close_end_1",
                "close_max_1",
                "score",
                "trend_tag",
                "date_generated",
            ]
        ],
        use_container_width=True,
    )
    st.info("Key Variables Explained")
    st.info(
        """
            ### 🧠 Breakdown of Key Fields Used in Scoring

            **1. `volume_0` and `volume_1`**  
            - `volume_0`: Total volume traded in **period_0** (older 10 days)  
            - `volume_1`: Total volume traded in **period_1** (recent 10 days)  
            ✅ You compare these to detect **rising demand**.

            ---

            **2. `trades_0` and `trades_1`**  
            - `trades_0`: Number of trades (executions) in the **older period**  
            - `trades_1`: Same for the **recent period**  
            ✅ An increase suggests **more market activity** or **algorithmic buying**, especially if volume is rising too.

            ---

            **3. `avg_change_0` and `avg_change_1`**  
            - `avg_change_0`: Average daily % price change in **older period**  
            - `avg_change_1`: Same but for **recent period**  
            ✅ Helps detect **momentum flips**:  
            If `avg_change_0` < 0 and `avg_change_1` > 0 → possible **institutional entry** or **price reversal**  
            ➡️ This triggers the `price_flip_up` flag.

            ---

            **4. `close_start_1` and `close_end_1`**  
            - `close_start_1`: Closing price on **first day** of period_1  
            - `close_end_1`: Closing price on **last day** of period_1  
            ✅ Used to measure **net price gain/loss** in the recent 10 days.

            ---

            **5. `close_max_1`**  
            - Highest close in **period_1**  
            ✅ If `close_end_1` ≥ `close_max_1`, it indicates a **recent high close** → potential **breakout**.
            """
    )
# === 📘 WEEKLY INTELLIGENCE (10-DAY) ===
elif page == "📘 Weekly Intelligence (10-Day)":
    st.subheader("📘 Weekly Trade Intelligence (Last 10 Days)")
    col1, col2, col3 = st.columns(3)

    conn = sqlite3.connect(DB_PATH)
    df_intel_short = pd.read_sql(
        """
        SELECT * FROM weekly_intel_short_100
        WHERE score >= 1 and avg_change_1 > avg_change_0 and avg_vol_1 > avg_vol_0 and close_end_1 > close_start_1
        ORDER BY name ASC
    """,
        conn,
    )
    conn.close()

    if df_intel_short.empty:
        st.info(
            "No weekly intelligence data available. Run weekly_intel_short.py first."
        )
    else:
        st.caption(
            "🚨 Highlighting weekly trade and volume anomalies to help track persistent activity."
        )

        with col1:
            selected_signal = st.selectbox(
                "Filter by Signal Type",
                ["All"]
                + sorted(df_intel_short["trend_tag"].dropna().unique().tolist()),
            )

        with col2:
            selected_date = st.selectbox(
                "Filter by Date",
                ["All"]
                + sorted(
                    df_intel_short["date_generated"].dropna().unique().tolist(),
                    reverse=True,
                ),
            )

        with col3:
            selected_name = st.selectbox(
                "Filter by Name",
                ["All"] + sorted(df_intel_short["name"].dropna().unique().tolist()),
            )

        filtered_df_short = df_intel_short.copy()
        if selected_signal != "All":
            filtered_df_short = filtered_df_short[
                filtered_df_short["trend_tag"] == selected_signal
            ]
        if selected_date != "All":
            filtered_df_short = filtered_df_short[
                filtered_df_short["date_generated"] == selected_date
            ]
        if selected_name != "All":
            filtered_df_short = filtered_df_short[
                filtered_df_short["name"] == selected_name
            ]

        # Columns to format
        volume_cols = [
            "volume_0",
            "volume_1",
        ]
        percent_cols = ["avg_change_0", "avg_change_1"]

        for col in volume_cols:
            if col in filtered_df_short.columns:
                filtered_df_short[col] = filtered_df_short[col].apply(human_readable)

        for col in percent_cols:
            if col in filtered_df_short.columns:
                filtered_df_short[col] = filtered_df_short[col].apply(format_percent)

        st.dataframe(
            filtered_df_short[
                [
                    "name",
                    "volume_0",
                    "volume_1",
                    "volume_spike",
                    "avg_change_0",
                    "avg_change_1",
                    "close_start_1",
                    "close_end_1",
                    "close_max_1",
                    "score",
                    "trend_tag",
                    "date_generated",
                ]
            ],
            use_container_width=True,
        )

    st.info("Key Variables Explained")
    st.info(
        """
### ✅ How to Use 30-Day & 10-Day Intelligence Together
            
### ✅ 10-Day Condition Guide (Quick Actions)

| **Condition in 10-Day View**                        | **What It Likely Means**         | **Suggested Action**          |
|-----------------------------------------------------|----------------------------------|-------------------------------|
| 📈 `avg_change_1` still **positive**                | Price trend is still intact      | ✅ **Hold**                   |
| 🔄 `avg_change_1` flattening, volume normal         | Minor pause/consolidation        | 😌 **Wait** for follow-up     |
| 📉 `avg_change_1` turns **negative**, volume spikes | Exit pressure building           | ⚠️ **Consider trimming**      |
| 🧘 Volume drops, price stable                       | Quiet zone, no panic             | ✅ **Hold** *(patience)*      |

---

### 🧠 Bonus Mental Model

- **30D** is your *anchor* — the big trend.  
- **10D** is your *compass* — the short-term pulse.

As long as your compass doesn’t swing violently in the opposite direction, **stay the course**.  
Don’t let a short-term flicker override the long-term map.

#### 1. 🔍 Signal Confirmation (Entry)

| Scenario | Action |
|----------|--------|
| Both 30D and 10D show volume spike, positive avg change, buildup or trend tag | ✅ **High-confidence entry** — strong conviction across timeframes |
| 30D = silent, 10D = signal spike | ⚠️ **Fast move or news-driven**; enter smaller or wait for 30D alignment |
| 30D = bullish, 10D = neutral or slightly down | 🤏 **Early dip-buy** opportunity (if price holds support) |
| 30D = bullish, 10D = already spiked hard | 🧘 **Wait for retracement** — may be late |

#### 2. 🚪 Exit Strategy

| Situation | Suggested Exit Action |
|-----------|------------------------|
| 10D shows falling avg_change_1, volume spike, trend_tag = 'distribution' while 30D is still up | 🏃 **Trim profits or exit partially** — short-term exit warning |
| Both 30D and 10D show flattening volume, avg_change_1 drops, and close_end_1 < close_start_1 | 🔚 **Consider full exit** — trend fading |
| 30D still strong, 10D consolidating | 😎 **Hold position** — patience can reward |
| Sudden 10D spike, no 30D support | ⚠️ **Be cautious** — likely a fakeout or pump |

#### 3. 🧠 Strategy Suggestions

- 🧲 **Swing Trading (1–2 weeks):**
- Enter only if **30D + 10D both show strength**
- Use 10D **weakness** as exit timing
- Watch for **repeat signals** across both frames

- 🏃 **Short-Term Scalps (1–3 days):**
- Use **10D only**
- Entry = **volume spike + avg_change_1 > 1.5%**
- Exit = when **10D momentum slows** OR **price hits recent 30D high**

#### 4. 🎯 BONUS: “Cross-Fade” Detection Strategy

- 📈 30D rising + 📉 10D falling → **Possible exit/retest**
- 📉 30D falling + 📈 10D rising → **Early reversal forming** (watch closely)

#### 👀 What to Watch in the Dashboard

| Column | How to Use |
|--------|------------|
| `avg_vol_1` vs `avg_vol_0` | Institutional interest change |
| `avg_change_1` | Momentum — is it increasing? |
| `close_max_1` vs `close_end_1` | Has price peaked and pulled back? |
| `trend_tag` | Buildup, spike, distribution — key insights |
| `score` | Quick way to sort for conviction |
"""
    )

elif page == "📊 Comparison Insights":
    st.subheader("📊 Comparison of 30-Day vs 10-Day Trends")

    try:
        # Load the CSV generated by intel_comparator.py
        df_compare = pd.read_csv("intel_comparison_report_100.csv")

        # Sidebar filters
        with st.sidebar:
            st.markdown("### 🔎 Filter")
            names = ["All"] + sorted(df_compare["name"].unique())
            selected_name = st.selectbox("Stock Name", names)

            selected_status = st.selectbox(
                "Signal Status", ["All"] + sorted(df_compare["status"].unique())
            )

        # Apply filters
        filtered_df = df_compare.copy()
        if selected_name != "All":
            filtered_df = filtered_df[filtered_df["name"] == selected_name]
        if selected_status != "All":
            filtered_df = filtered_df[filtered_df["status"] == selected_status]

        # Columns to format
        volume_cols = ["vol_30", "vol_10"]
        percent_cols = ["change_30", "change_10"]

        for col in volume_cols:
            if col in filtered_df.columns:
                filtered_df[col] = filtered_df[col].apply(human_readable)

        for col in percent_cols:
            if col in filtered_df.columns:
                filtered_df[col] = filtered_df[col].apply(format_percent)

        st.markdown(f"Showing {len(filtered_df)} entries")
        st.dataframe(filtered_df, use_container_width=True)

    except FileNotFoundError:
        st.warning(
            "❌ No comparison report found. Run intel_comparator.py to generate the CSV."
        )


if page == "Match View Strong":

    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT 
        ic.name,
        ic.change_30,
        ic.change_10,
        ic.status,
        s.date,
        s.signal,
        s.action,
        s.confidence_score,
        s.close,
        s.buy_range
    FROM 
        intel_comparison_100 AS ic
    JOIN (
        SELECT * FROM signals_100 
        WHERE 
            action IN ('BUY', 'BUY SMALL', 'BUY CONFIRMED') 
            AND date >= DATE('now', '-3 days') 
            AND date < DATE('now')
    ) AS s
    ON ic.name = s.name
    WHERE 
        ic.status LIKE '%strong uptrend%'
        AND s.date = (
            SELECT MAX(date) 
            FROM signals_100 
            WHERE 
                name = s.name 
                AND action IN ('BUY', 'BUY SMALL', 'BUY CONFIRMED') 
                AND date >= DATE('now', '-7 days') 
                AND date < DATE('now')
        )
    ORDER BY 
        s.date DESC
    """

    df = pd.read_sql(query, conn)

    col1, col2 = st.columns(2)

    with col1:
        selected_date = st.selectbox(
            "Filter by Date",
            ["All"] + sorted(df["date"].unique().tolist(), reverse=True),
        )

    with col2:
        selected_name = st.selectbox(
            "Filter by Name", ["All"] + sorted(df["name"].unique().tolist())
        )

    # === Apply filters ===
    filtered_df = df.copy()

    if selected_date != "All":
        filtered_df = filtered_df[filtered_df["date"] == selected_date]

    if selected_name != "All":
        filtered_df = filtered_df[filtered_df["name"] == selected_name]

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
                "change_30",
                "change_10",
                "buy_range",
                "close",
                "confidence_score",
                "action",
                "status",
                "signal",
            ]
        ].style.apply(highlight_row, axis=1),
        use_container_width=True,
    )
if page == "Match View Pull Back":

    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT 
        ic.name,
        ic.change_30,
        ic.change_10,
        ic.status,
        s.date,
        s.signal,
        s.action,
        s.confidence_score,
        s.close,
        s.buy_range
    FROM 
        intel_comparison_100 AS ic 
    JOIN (
        SELECT * FROM signals_100 
        WHERE 
            action IN ('BUY', 'BUY SMALL', 'BUY CONFIRMED') 
            AND date >= DATE('now', '-3 days') 
            AND date < DATE('now')
    ) AS s
    ON ic.name = s.name
    WHERE 
        ic.status LIKE '%10D Weakness%'
        AND s.date = (
            SELECT MAX(date) 
            FROM signals_100 
            WHERE 
                name = s.name 
                AND action IN ('BUY', 'BUY SMALL', 'BUY CONFIRMED') 
                AND date >= DATE('now', '-7 days') 
                AND date < DATE('now')
        )
    ORDER BY 
        s.date DESC
    """

    df = pd.read_sql(query, conn)

    col1, col2 = st.columns(2)

    with col1:
        selected_date = st.selectbox(
            "Filter by Date",
            ["All"] + sorted(df["date"].unique().tolist(), reverse=True),
        )

    with col2:
        selected_name = st.selectbox(
            "Filter by Name", ["All"] + sorted(df["name"].unique().tolist())
        )

    # === Apply filters ===
    filtered_df = df.copy()

    if selected_date != "All":
        filtered_df = filtered_df[filtered_df["date"] == selected_date]

    if selected_name != "All":
        filtered_df = filtered_df[filtered_df["name"] == selected_name]

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
                "change_30",
                "change_10",
                "buy_range",
                "close",
                "confidence_score",
                "action",
                "status",
                "signal",
            ]
        ].style.apply(highlight_row, axis=1),
        use_container_width=True,
    )
