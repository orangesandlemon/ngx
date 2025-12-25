# app.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os


st.set_page_config(page_title="US Signal Dashboard", layout="wide")

DB_PATH = "data/us_equities.db"


# === Load signals ===
@st.cache_data
def load_signals():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        "SELECT * FROM signals_100  WHERE date > date('now', '-30 day')  ORDER BY date DESC",
        conn,
    )
    conn.close()
    return df


df = load_signals()


def highlight_row(row):
    if "Limit-Up" in row["signal"] or row["action"] in ["BUY CONFIRMED", "BUY SMALL"]:
        return ["background-color: #fff7e6"] * len(row)  # Light gold
    else:
        return [""] * len(row)


# === Filters ===
st.title("📈 US Signal Tracker")
st.caption("Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))


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


page = st.radio(
    "Choose a section:",
    [
        "🔍 Signals",
        "🕵️ Institutional Watchlist",
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
                "signal_tier",
            ]
        ].style.apply(highlight_row, axis=1),
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
