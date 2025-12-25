# app.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import plotly.express as px
import os
import plotly.graph_objects as go


st.set_page_config(page_title="NGX Signal Dashboard", layout="wide")

DB_PATH = "data/ngx_equities.db"


def highlight_row(row):
    if "Limit-Up" in row["signal"] or row["action"] in ["BUY CONFIRMED", "BUY SMALL"]:
        return ["background-color: #fff7e6"] * len(row)  # Light gold
    else:
        return [""] * len(row)


# === Filters ===
st.title("📈 NGX Signal Tracker")
st.caption("Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))

st.subheader("📈 3-Day Accumulation / Distribution Signals")

# Load new signal table
conn = sqlite3.connect("data/ngx_equities.db")
df = pd.read_sql("SELECT * FROM accumulation_signals_3day  ", conn)
conn.close()

# Detect price flatness (same value for all 3 days)
price_cols = [col for col in df.columns if col.startswith("price_")]
df["price_flat"] = df[price_cols].nunique(axis=1) == 1

# Check that volume data exists
volume_cols = [col for col in df.columns if col.startswith("volume_")]
df["volume_nonnull"] = df[volume_cols].apply(
    lambda row: all(x > 0 for x in row), axis=1
)

# Optional: sort by value or volume for better signal visibility
volume_cols = [col for col in df.columns if col.startswith("volume_")]
df["avg_volume"] = df[volume_cols].mean(axis=1)
df = df.sort_values(by="avg_volume", ascending=False)

# Add implied price and discrepancy calculations first
df["implied_price"] = df["value_total"] / df["volume_total"]
df["price_discrepancy_ratio"] = df["implied_price"] / df["price"]

# Optional flag column
df["discrepancy_flag"] = df["price_discrepancy_ratio"].apply(
    lambda x: (
        "High Implied Price" if x > 1.5 else "Low Implied Price" if x < 0.5 else ""
    )
)
# 2. 🧠 Reorder columns: move implied and discrepancy fields right after 'signal'
reorder_cols = df.columns.tolist()
signal_index = reorder_cols.index("signal") if "signal" in reorder_cols else -1

if signal_index != -1:
    extra_cols = ["implied_price", "price_discrepancy_ratio", "discrepancy_flag"]
    extra_cols_present = [col for col in extra_cols if col in df.columns]

    # Remove them first
    for col in extra_cols_present:
        reorder_cols.remove(col)

    # Insert after 'signal'
    for i, col in enumerate(extra_cols_present):
        reorder_cols.insert(signal_index + 1 + i, col)

    df = df[reorder_cols]

flat_price_only = st.checkbox("📉 Show only flat-price stocks with volume activity")


if flat_price_only:

    # Now apply the filters
    df = df[
        df["price_flat"].astype(bool)
        & df["volume_nonnull"].astype(bool)
        & ~df["signal"].isin(["accumulation", "distribution"])
    ]


# Format numbers
volume_cols = [col for col in df.columns if col.startswith("volume_")]
value_cols = [col for col in df.columns if col.startswith("value_")]
price_cols = [col for col in df.columns if col.startswith("price_")]

# Format volume: integer with commas
for col in volume_cols:
    df[col] = df[col].astype(int).apply(lambda x: f"{x:,}")

# Format value and price: 2 decimal places with commas
for col in value_cols + price_cols:
    df[col] = df[col].apply(lambda x: f"{x:,.2f}")


# Highlight signal
def highlight_signal(row):
    color = {
        "accumulation": "lightgreen",
        "distribution": "lightcoral",
        "neutral": "",
    }.get(row["signal"], "")
    return [f"background-color: {color}" for _ in row]


# Dropdown filter by sub_sector (optional)
# Dropdown for subsector filter
sector_filter = st.selectbox(
    "📂 Filter by Sub-Sector", ["All"] + sorted(df["sub_sector"].dropna().unique())
)

# Dropdown for signal type filter
signal_filter = st.selectbox(
    "📶 Filter by the Signal", ["All", "accumulation", "distribution", "neutral"]
)

# Apply filters
if sector_filter != "All":
    df = df[df["sub_sector"] == sector_filter]

if signal_filter != "All":
    df = df[df["signal"] == signal_filter]

# Display styled table
st.dataframe(df.style.apply(highlight_signal, axis=1), use_container_width=True)


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


# === Load signals ===
@st.cache_data
def load_signals():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM signals where date >= '2025-11-01' ORDER BY date DESC", conn)
    conn.close()
    return df


df = load_signals()

page = st.radio(
    "Choose a section:",
    [
        "🔍 Signals",
        "📈 Price Change Patterns",
        "📊 Weekly Intelligence",
        "📘 Weekly Intelligence (10-Day)",
        "📊 Volume–Value Visualizer",
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
elif page == "📈 Price Change Patterns":
    st.subheader("📈 Price Change (%) Over Time")

    conn = sqlite3.connect(DB_PATH)
    df_pct = pd.read_sql(
        "SELECT name, date, change_pct, close, volume FROM equities where volume > 0 AND date >= '2025-09-01' ORDER BY date DESC",
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

    # Volume bars on a 3rd axis (offset to the right) so they don't affect ax1 scaling
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("axes", 1.12))  # push outward
    ax3.bar(
        zoomed_df["date"],
        zoomed_df["volume"],
        width=0.8,
        color="gray",
        alpha=0.22,  # faint background
        zorder=0,
    )
    ax3.set_yticks([])  # keep unobtrusive
    ax3.set_ylabel("")  # hide label
    ax3.grid(False)

    fig.autofmt_xdate()
    fig.tight_layout()
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

        # Volume bars on 3rd axis so they don't affect % change scale
        ax3 = ax1.twinx()
        ax3.spines["right"].set_position(("axes", 1.10))  # small offset
        ax3.bar(
            stock_df["date"],
            stock_df["volume"],
            width=0.8,
            color="gray",
            alpha=0.22,
            zorder=0,
        )
        ax3.set_yticks([])
        ax3.grid(False)

        ax1.set_title(stock, fontsize=10)
        ax1.set_xticks([])  # hide x-axis ticks
        ax2.set_xticks([])

        fig.tight_layout()

        # Show in alternating columns
        with cols[col_idx]:
            st.pyplot(fig)

        col_idx = (col_idx + 1) % 2  # Switch column


elif page == "📊 Weekly Intelligence":
    st.subheader("📊 Weekly Trade Intelligence (Last 30 Days)")
    col1, col2, col3 = st.columns(3)

    conn = sqlite3.connect(DB_PATH)
    df_intel = pd.read_sql(
        """
        SELECT * FROM weekly_intel
        WHERE score >= 1 and avg_change_1 > avg_change_0 and volume_1 > volume_0 and close_end_1 > close_start_1 and date_generated >= '2025-09-01'
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
                "trades_0",
                "trades_1",
                "trade_spike",
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
        SELECT * FROM weekly_intel_short
        WHERE score >= 1 and avg_change_1 > avg_change_0 and volume_1 > volume_0 and close_end_1 > close_start_1 and date_generated >= '2025-11-01'
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
                    "trades_0",
                    "trades_1",
                    "trade_spike",
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
elif page == "📊 Volume–Value Visualizer":
    st.subheader("📊 Volume–Value Visualizer")

    view_mode = st.radio(
        "Select View:",
        [
            "📈 Stock Trend (Last 20 Days)",
            "🔍 Divergence by Date",
            "📊 Sector Divergence",
            "🔍 Sector Stock Divergence",
        ],
        horizontal=True,
    )
    conn = sqlite3.connect("data/ngx_equities.db")

    if view_mode == "📈 Stock Trend (Last 20 Days)":
        stock_name = st.selectbox(
            "Select Stock",
            pd.read_sql(
                "SELECT DISTINCT name FROM equities WHERE volume > 0  and date >= '2025-09-01' ORDER BY name ",
                conn,
            )["name"],
        )

        # Query last 20 days for selected stock
        # Correct query
        query = """
            SELECT date, MAX(close) as Price, SUM(volume) as Volume, SUM(value) as Value
            FROM equities
            WHERE name = ? AND volume > 0 
            GROUP BY date
            ORDER BY date DESC
            LIMIT 15
        """
        df_stock = pd.read_sql(query, conn, params=(stock_name,))
        df_stock = df_stock.sort_values("date")

        # Scale values
        df_stock["volume_b"] = df_stock["Volume"]
        df_stock["value_b"] = df_stock["Value"]

        # Build chart
        fig = go.Figure()

        # Bar for Volume
        fig.add_trace(
            go.Bar(
                x=df_stock["date"],
                y=df_stock["Volume"],
                name="Volume (₦B)",
                marker_color="red",
                yaxis="y",
            )
        )

        # Bar for Value
        fig.add_trace(
            go.Bar(
                x=df_stock["date"],
                y=df_stock["Value"],
                name="Value (₦B)",
                marker_color="green",
                yaxis="y",
            )
        )

        # Line for Close Price
        fig.add_trace(
            go.Scatter(
                x=df_stock["date"],
                y=df_stock["Price"],
                name="Close Price (₦)",
                mode="lines+markers",
                line=dict(color="orange", width=3),
                yaxis="y2",
            )
        )

        # Layout
        fig.update_layout(
            title=f"{stock_name} - Volume, Value, and Close Price (Last 15 Days)",
            xaxis=dict(title="Date"),
            yaxis=dict(title="Volume / Value (₦)", side="left"),
            yaxis2=dict(
                title="Price (₦)", overlaying="y", side="right", showgrid=False
            ),
            barmode="group",
            height=500,
            legend=dict(orientation="h", y=1.1, x=1, xanchor="right"),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Divider
        st.markdown("---")
        st.subheader("🔍 Quick View: All Stocks (Mini Charts)")

        # Get list of all stocks with volume > 0
        all_stocks = pd.read_sql(
            "SELECT DISTINCT name FROM equities WHERE volume > 0 ORDER BY name", conn
        )["name"].tolist()

        # Columns: 2 per row
        col1, col2 = st.columns(2)

        # Helper function to build mini chart
        def plot_mini_chart(stock):
            query = """
                SELECT date, MAX(close) as Price, SUM(volume) as Volume, SUM(value) as Value
                FROM equities
                WHERE name = ? AND volume > 0 
                GROUP BY date
                ORDER BY date DESC
                LIMIT 15
            """
            df = pd.read_sql(query, conn, params=(stock,))
            df = df.sort_values("date")
            df["Price_Change"] = df["Price"].diff()
            df["Value_Color"] = df["Price_Change"].apply(
                lambda x: "green" if x >= 0 else "blue"
            )

            fig = go.Figure()

            fig.add_trace(
                go.Bar(
                    x=df["date"],
                    y=df["Volume"],
                    name="Volume",
                    marker_color="red",
                    yaxis="y",
                )
            )
            # Plot Value (dynamic color)
            fig.add_trace(
                go.Bar(
                    x=df["date"],
                    y=df["Value"],
                    name="Value",
                    marker=dict(color=df["Value_Color"]),
                    yaxis="y",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["Price"],
                    name="Price",
                    mode="lines+markers",
                    line=dict(color="orange", width=2),
                    yaxis="y2",
                )
            )

            fig.update_layout(
                title=stock,
                xaxis=dict(title=None),
                yaxis=dict(title="Vol/Val", side="left"),
                yaxis2=dict(
                    title="Price", overlaying="y", side="right", showgrid=False
                ),
                barmode="group",
                height=300,
                margin=dict(l=20, r=20, t=30, b=20),
                showlegend=False,
            )

            return fig

        # Loop through stocks and display in two columns
        for i, stock in enumerate(all_stocks):
            if i % 2 == 0:
                with col1:
                    st.plotly_chart(plot_mini_chart(stock), use_container_width=True)
            else:
                with col2:
                    st.plotly_chart(plot_mini_chart(stock), use_container_width=True)

    elif view_mode == "🔍 Divergence by Date":
        selected_day = st.selectbox(
            "Select Date",
            pd.read_sql("SELECT DISTINCT date FROM equities where date >= 2025-09-01 ORDER BY date DESC", conn)[
                "date"
            ],
        )

        query = f"""
        SELECT name, close as Price, volume as Volume, value as Value
        FROM equities
        WHERE date = ?
        """
        df_day = pd.read_sql(query, conn, params=(selected_day,))

        # Calculate % difference
        df_day["divergence_pct"] = (
            abs(df_day["Volume"] - df_day["Value"])
            / df_day[["Volume", "Value"]].max(axis=1)
        ) * 100
        df_top10 = df_day.sort_values("divergence_pct", ascending=False).head(20)

        df_melted = df_top10.melt(
            id_vars="name",
            value_vars=["Price", "Volume", "Value"],
            var_name="Metric",
            value_name="Amount",
        )

        fig = px.bar(
            df_melted,
            x="name",
            y="Amount",
            color="Metric",
            color_discrete_map={"Price": "yellow", "Volume": "red", "Value": "green"},
            title=f"Top 10 Stocks by Volume–Value Divergence on {selected_day}",
        )
        fig.update_layout(
            barmode="stack", xaxis_title="Stock", yaxis_title="Raw Value", height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    elif view_mode == "📊 Sector Divergence":
        selected_day = st.selectbox(
            "Select Date",
            pd.read_sql("SELECT DISTINCT date FROM equities ORDER BY date DESC", conn)[
                "date"
            ],
            key="sector_divergence_date",
        )

        # Toggle divergence metric
        divergence_type = st.radio(
            "Divergence Metric",
            ["Percentage (%)", "Raw (₦)"],
            horizontal=True,
            key="sector_div_metric",
        )

        try:
            # --- Helper: compute divergence for a sector dataframe with total_volume/total_value ---
            def add_divergence_cols(df):
                if df.empty:
                    df["divergence_pct"] = []
                    df["divergence_raw"] = []
                    df["divergence_metric_plot"] = []
                    df["_div_label"] = []
                    return df
                df["divergence_pct"] = (
                    (df["total_volume"] - df["total_value"]).abs()
                    / df[["total_volume", "total_value"]].max(axis=1)
                    * 100
                )
                df["divergence_raw"] = (df["total_volume"] - df["total_value"]).abs()
                # For plotting: if raw, show in ₦ millions; if %, show % as-is
                if divergence_type == "Percentage (%)":
                    df["divergence_metric_plot"] = df["divergence_pct"]
                    df["_div_label"] = "Divergence (%)"
                else:
                    df["divergence_metric_plot"] = df["divergence_raw"] / 1_000_000.0
                    df["_div_label"] = "Divergence (₦ Millions)"
                return df

            # --- MAIN: totals per sector on selected day ---
            df_sector = pd.read_sql(
                """
                SELECT main_sector, SUM(volume) as total_volume, SUM(value) as total_value
                FROM equities
                WHERE date = ?
                GROUP BY main_sector
                """,
                conn,
                params=(selected_day,),
            )
            df_sector = add_divergence_cols(df_sector)

            # Sort & take Top 10 by chosen metric (pct or raw)
            sort_key = (
                "divergence_pct"
                if divergence_type == "Percentage (%)"
                else "divergence_raw"
            )
            df_top_sectors = (
                df_sector.sort_values(sort_key, ascending=False).head(10).copy()
            )
            top_sector_order = df_top_sectors["main_sector"].tolist()

            # Prepare display columns (scale totals to ₦ millions for readability)
            df_disp = df_top_sectors.copy()
            df_disp["Volume (₦m)"] = df_disp["total_volume"] / 1_000_000.0
            df_disp["Value (₦m)"] = df_disp["total_value"] / 1_000_000.0

            df_melted = df_disp.melt(
                id_vars="main_sector",
                value_vars=["Volume (₦m)", "Value (₦m)"],
                var_name="Metric",
                value_name="Amount",
            )

            # MAIN chart: Volume vs Value (in ₦m) for the selected day (Top 10 sectors by chosen divergence)
            fig = px.bar(
                df_melted,
                x="main_sector",
                y="Amount",
                color="Metric",
                category_orders={"main_sector": top_sector_order},
                title=f"Top 10 Sectors by Volume–Value Divergence on {selected_day} ({'%' if divergence_type=='Percentage (%)' else 'Raw ₦'})",
                color_discrete_map={"Volume (₦m)": "blue", "Value (₦m)": "green"},
            )
            fig.update_layout(
                barmode="group",
                xaxis_title="Sector",
                yaxis_title="Amount (₦ Millions)",
                height=500,
            )
            st.plotly_chart(fig, use_container_width=True)

            # === FAST VOLUME ROTATION: 1d vs trailing 3d (shares) ===
            st.subheader("Fast Rotation — Volume (1d vs trailing 3d)")

            # Get the previous 3 trading days before the selected day
            prev3 = pd.read_sql(
                """
                SELECT DISTINCT date
                FROM equities
                WHERE date < ?
                ORDER BY date DESC
                LIMIT 3
                """,
                conn,
                params=(selected_day,),
            )["date"].tolist()

            def sector_volume_share(dates_list):
                """Return sector volume share (%) aggregated over the given dates."""
                if not dates_list:
                    return pd.DataFrame(
                        columns=["main_sector", "total_volume", "vol_share"]
                    )
                placeholders = ",".join(["?"] * len(dates_list))
                dfv = pd.read_sql(
                    f"""
                    SELECT main_sector, SUM(volume) AS total_volume
                    FROM equities
                    WHERE date IN ({placeholders})
                    GROUP BY main_sector
                    """,
                    conn,
                    params=dates_list,
                )
                total = dfv["total_volume"].sum()
                dfv["vol_share"] = (dfv["total_volume"] / total * 100) if total else 0.0
                return dfv

            df_today = sector_volume_share([selected_day])
            df_trail3 = sector_volume_share(prev3)

            if df_today.empty or df_trail3.empty:
                st.info("Not enough data for 1d vs trailing 3d volume view.")
            else:
                # Keep same Top 10 sectors from your main chart if available; fallback to today's top by volume
                if "top_sector_order" in locals() and top_sector_order:
                    keep = top_sector_order
                else:
                    keep = (
                        df_today.sort_values("total_volume", ascending=False)[
                            "main_sector"
                        ]
                        .head(10)
                        .tolist()
                    )

                df_today = df_today[df_today["main_sector"].isin(keep)]
                df_trail3 = df_trail3[df_trail3["main_sector"].isin(keep)]

                # Compute change in volume share (percentage points)
                df_delta = (
                    df_today[["main_sector", "vol_share"]]
                    .merge(
                        df_trail3[["main_sector", "vol_share"]],
                        on="main_sector",
                        how="outer",
                        suffixes=("_today", "_trail3"),
                    )
                    .fillna(0)
                )

                df_delta["pp_change"] = (
                    df_delta["vol_share_today"] - df_delta["vol_share_trail3"]
                )
                df_delta = df_delta.sort_values("pp_change", ascending=False)

                # Lock the display order to the sorted result
                df_delta["main_sector"] = pd.Categorical(
                    df_delta["main_sector"],
                    categories=df_delta["main_sector"].tolist(),
                    ordered=True,
                )

                # Plot: who is gaining/losing volume share today vs trailing 3
                fig_vol_fast = px.bar(
                    df_delta,
                    x="main_sector",
                    y="pp_change",
                    color=(df_delta["pp_change"] > 0),
                    color_discrete_map={True: "green", False: "red"},
                    title=f"Sector Volume Share Δ — Today vs Trailing 3d (ending {selected_day})",
                )
                fig_vol_fast.update_layout(
                    xaxis_title="Sector",
                    yaxis_title="Change in volume share (pp)",
                    height=420,
                    showlegend=False,
                )
                st.plotly_chart(fig_vol_fast, use_container_width=True)

        except Exception as e:
            st.warning("Error occurred while retrieving sector divergence data.")
            st.text(str(e))

        # =========================
        # Volume rotation add-ons
        # =========================

        st.subheader("Fast Rotation — Volume (3d vs prior 3d) & Standard (5d vs 6–10)")

        def sector_volume_share(dates_list):
            """Return sector volume share (%) aggregated over given dates."""
            if not dates_list:
                return pd.DataFrame(
                    columns=["main_sector", "total_volume", "vol_share"]
                )
            placeholders = ",".join(["?"] * len(dates_list))
            dfv = pd.read_sql(
                f"""
                SELECT main_sector, SUM(volume) AS total_volume
                FROM equities
                WHERE date IN ({placeholders})
                GROUP BY main_sector
                """,
                conn,
                params=dates_list,
            )
            total = dfv["total_volume"].sum()
            dfv["vol_share"] = (dfv["total_volume"] / total * 100) if total else 0.0
            return dfv

        # Get up to last 10 trading days up to selected_day (inclusive)
        dates_10_all = pd.read_sql(
            """
            SELECT DISTINCT date
            FROM equities
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT 10
            """,
            conn,
            params=(selected_day,),
        )["date"].tolist()

        # Helper to pick display order (prefer your Top 10 sectors if available)
        def choose_keep(df_pref, fallback=10):
            if "top_sector_order" in locals() and top_sector_order:
                return top_sector_order
            if df_pref is not None and not df_pref.empty:
                return (
                    df_pref.sort_values("total_volume", ascending=False)["main_sector"]
                    .head(fallback)
                    .tolist()
                )
            return []

        # ---------- FAST: 3d vs prior 3d ----------
        if len(dates_10_all) >= 6:
            recent3 = dates_10_all[:3]  # selected day back 2 more
            prior3 = dates_10_all[3:6]  # the 3 days before that

            df_recent3 = sector_volume_share(recent3)
            df_prior3 = sector_volume_share(prior3)

            keep_3 = choose_keep(df_recent3, fallback=10)
            if keep_3:
                df_recent3 = df_recent3[df_recent3["main_sector"].isin(keep_3)]
                df_prior3 = df_prior3[df_prior3["main_sector"].isin(keep_3)]

            df_delta3 = (
                df_recent3[["main_sector", "vol_share"]]
                .merge(
                    df_prior3[["main_sector", "vol_share"]],
                    on="main_sector",
                    how="outer",
                    suffixes=("_recent3", "_prior3"),
                )
                .fillna(0)
            )

            df_delta3["pp_change"] = (
                df_delta3["vol_share_recent3"] - df_delta3["vol_share_prior3"]
            )
            df_delta3 = df_delta3.sort_values("pp_change", ascending=False)
            df_delta3["main_sector"] = pd.Categorical(
                df_delta3["main_sector"],
                categories=df_delta3["main_sector"].tolist(),
                ordered=True,
            )

            fig_vol_fast3 = px.bar(
                df_delta3,
                x="main_sector",
                y="pp_change",
                color=(df_delta3["pp_change"] > 0),
                color_discrete_map={True: "green", False: "red"},
                title=f"Sector Volume Share Δ — Recent 3d vs Prior 3d (ending {recent3[0]})",
            )
            fig_vol_fast3.update_layout(
                xaxis_title="Sector",
                yaxis_title="Change in volume share (pp)",
                height=420,
                showlegend=False,
            )
            st.plotly_chart(fig_vol_fast3, use_container_width=True)
        else:
            st.info("Not enough history for Fast (3d vs prior 3d) volume view.")

        # ---------- STANDARD: 5d vs 6–10 ----------
        if len(dates_10_all) >= 10:
            recent5 = dates_10_all[:5]
            base5 = dates_10_all[5:10]

            df_recent5 = sector_volume_share(recent5)
            df_base5 = sector_volume_share(base5)

            keep_5 = choose_keep(df_recent5, fallback=10)
            if keep_5:
                df_recent5 = df_recent5[df_recent5["main_sector"].isin(keep_5)]
                df_base5 = df_base5[df_base5["main_sector"].isin(keep_5)]

            df_delta5 = (
                df_recent5[["main_sector", "vol_share"]]
                .merge(
                    df_base5[["main_sector", "vol_share"]],
                    on="main_sector",
                    how="outer",
                    suffixes=("_recent5", "_base5"),
                )
                .fillna(0)
            )

            df_delta5["pp_change"] = (
                df_delta5["vol_share_recent5"] - df_delta5["vol_share_base5"]
            )
            df_delta5 = df_delta5.sort_values("pp_change", ascending=False)
            df_delta5["main_sector"] = pd.Categorical(
                df_delta5["main_sector"],
                categories=df_delta5["main_sector"].tolist(),
                ordered=True,
            )

            fig_vol_std5 = px.bar(
                df_delta5,
                x="main_sector",
                y="pp_change",
                color=(df_delta5["pp_change"] > 0),
                color_discrete_map={True: "green", False: "red"},
                title=f"Sector Volume Share Δ — Recent 5d vs Days 6–10 (ending {recent5[0]})",
            )
            fig_vol_std5.update_layout(
                xaxis_title="Sector",
                yaxis_title="Change in volume share (pp)",
                height=420,
                showlegend=False,
            )
            st.plotly_chart(fig_vol_std5, use_container_width=True)
        else:
            st.info("Not enough history for Standard (5d vs 6–10) volume view.")

    elif view_mode == "🔍 Sector Stock Divergence":
        selected_day = st.selectbox(
            "Select Date",
            pd.read_sql("SELECT DISTINCT date FROM equities ORDER BY date DESC", conn)[
                "date"
            ],
            key="sector_stock_date",
        )

        # Sector & Top-N
        sectors = pd.read_sql(
            "SELECT DISTINCT main_sector FROM equities ORDER BY main_sector", conn
        )["main_sector"].dropna()
        selected_sector = st.selectbox("Select Sector", sectors)
        top_n = st.radio("Number of Stocks", [5, 10], horizontal=True)

        # ===== MAIN (TODAY) — VOLUME ONLY, color by price direction =====
        df_today = pd.read_sql(
            """
            SELECT name,
                open  AS Open,
                close AS Close,
                volume AS Volume
            FROM equities
            WHERE date = ? AND main_sector = ?
            """,
            conn,
            params=(selected_day, selected_sector),
        )

        import plotly.graph_objects as go
        import plotly.express as px

        if df_today.empty:
            st.warning(f"No data found for {selected_sector} on {selected_day}")
            top_stock_order = []
        else:
            # Sort by Volume (desc) and take Top N
            df_top = df_today.sort_values("Volume", ascending=False).head(top_n).copy()
            top_stock_order = df_top["name"].tolist()

            unit_divisor = 1_000_000  # show volume in millions
            names = df_top["name"].tolist()
            vol_m = (df_top["Volume"] / unit_divisor).tolist()
            colors = [
                "red" if float(c) < float(o) else "green"
                for o, c in zip(df_top["Open"], df_top["Close"])
            ]

            fig_main = go.Figure()
            fig_main.add_trace(
                go.Bar(x=names, y=vol_m, name="Volume", marker_color=colors)
            )
            fig_main.update_layout(
                title=f"{selected_sector} — Top {top_n} by Volume • Today ({selected_day})",
                barmode="stack",
                height=480,
                legend_title_text="Metric",
            )
            fig_main.update_xaxes(title_text="Stock")
            fig_main.update_yaxes(title_text="Volume (Millions)")
            st.plotly_chart(fig_main, use_container_width=True)

        # ===== Helpers for rotation charts =====
        def stock_volume_share(dates_list):
            """Return stock volume share (%) within the selected sector over given dates."""
            if not dates_list:
                return pd.DataFrame(columns=["name", "total_volume", "vol_share"])
            ph = ",".join(["?"] * len(dates_list))
            dfv = pd.read_sql(
                f"""
                SELECT name, SUM(volume) AS total_volume
                FROM equities
                WHERE main_sector = ? AND date IN ({ph})
                GROUP BY name
                """,
                conn,
                params=[selected_sector, *dates_list],
            )
            total = dfv["total_volume"].sum()
            dfv["vol_share"] = (dfv["total_volume"] / total * 100) if total else 0.0
            return dfv

        def choose_keep(df_pref, fallback=10):
            # Prefer today's Top N list for consistent focus; otherwise fallback to volume leaders
            if df_pref is not None and not df_pref.empty:
                base = (
                    df_pref.sort_values("total_volume", ascending=False)["name"]
                    .head(fallback)
                    .tolist()
                )
            else:
                base = []
            return top_stock_order or base

        # ===== Fast Rotation — Volume (1d vs trailing 3d) =====
        st.subheader("Fast Rotation — Volume (1d vs trailing 3d)")
        prev3 = pd.read_sql(
            """
            SELECT DISTINCT date
            FROM equities
            WHERE date < ?
            ORDER BY date DESC
            LIMIT 3
            """,
            conn,
            params=(selected_day,),
        )["date"].tolist()

        df_today_vol = stock_volume_share([selected_day])
        df_trail3_vol = stock_volume_share(prev3)

        if df_today_vol.empty or df_trail3_vol.empty:
            st.info("Not enough data for 1d vs trailing 3d volume view.")
        else:
            keep = choose_keep(df_today_vol, fallback=top_n)
            df_today_vol = df_today_vol[df_today_vol["name"].isin(keep)]
            df_trail3_vol = df_trail3_vol[df_trail3_vol["name"].isin(keep)]

            df_delta_v13 = (
                df_today_vol[["name", "vol_share"]]
                .merge(
                    df_trail3_vol[["name", "vol_share"]],
                    on="name",
                    how="outer",
                    suffixes=("_today", "_trail3"),
                )
                .fillna(0)
            )
            df_delta_v13["pp_change"] = (
                df_delta_v13["vol_share_today"] - df_delta_v13["vol_share_trail3"]
            )
            df_delta_v13 = df_delta_v13.sort_values("pp_change", ascending=False)
            df_delta_v13["name"] = pd.Categorical(
                df_delta_v13["name"],
                categories=df_delta_v13["name"].tolist(),
                ordered=True,
            )

            fig_vol_fast = px.bar(
                df_delta_v13,
                x="name",
                y="pp_change",
                color=(df_delta_v13["pp_change"] > 0),
                color_discrete_map={True: "green", False: "red"},
                title=f"{selected_sector} — Volume Share Δ — Today vs Trailing 3d (ending {selected_day})",
            )
            fig_vol_fast.update_layout(
                xaxis_title="Stock",
                yaxis_title="Change in volume share (pp)",
                height=400,
                showlegend=False,
            )
            st.plotly_chart(fig_vol_fast, use_container_width=True)

        # ===== Fast Rotation — Volume (3d vs prior 3d) =====
        st.subheader("Fast Rotation — Volume (3d vs prior 3d)")
        dates_10_all = pd.read_sql(
            """
            SELECT DISTINCT date
            FROM equities
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT 10
            """,
            conn,
            params=(selected_day,),
        )["date"].tolist()

        if len(dates_10_all) >= 6:
            recent3 = dates_10_all[:3]
            prior3 = dates_10_all[3:6]

            df_recent3 = stock_volume_share(recent3)
            df_prior3 = stock_volume_share(prior3)

            keep3 = choose_keep(df_recent3, fallback=top_n)
            df_recent3 = df_recent3[df_recent3["name"].isin(keep3)]
            df_prior3 = df_prior3[df_prior3["name"].isin(keep3)]

            df_delta_v3 = (
                df_recent3[["name", "vol_share"]]
                .merge(
                    df_prior3[["name", "vol_share"]],
                    on="name",
                    how="outer",
                    suffixes=("_recent3", "_prior3"),
                )
                .fillna(0)
            )
            df_delta_v3["pp_change"] = (
                df_delta_v3["vol_share_recent3"] - df_delta_v3["vol_share_prior3"]
            )
            df_delta_v3 = df_delta_v3.sort_values("pp_change", ascending=False)
            df_delta_v3["name"] = pd.Categorical(
                df_delta_v3["name"],
                categories=df_delta_v3["name"].tolist(),
                ordered=True,
            )

            fig_vol_fast3 = px.bar(
                df_delta_v3,
                x="name",
                y="pp_change",
                color=(df_delta_v3["pp_change"] > 0),
                color_discrete_map={True: "green", False: "red"},
                title=f"{selected_sector} — Volume Share Δ — Recent 3d vs Prior 3d (ending {recent3[0]})",
            )
            fig_vol_fast3.update_layout(
                xaxis_title="Stock",
                yaxis_title="Change in volume share (pp)",
                height=400,
                showlegend=False,
            )
            st.plotly_chart(fig_vol_fast3, use_container_width=True)
        else:
            st.info("Not enough history for 3d vs prior 3d volume view.")

        # ===== Standard Rotation — Volume (5d vs 6–10) =====
        st.subheader("Standard Rotation — Volume (5d vs 6–10)")
        if len(dates_10_all) >= 10:
            recent5 = dates_10_all[:5]
            base5 = dates_10_all[5:10]

            df_recent5 = stock_volume_share(recent5)
            df_base5 = stock_volume_share(base5)

            keep5 = choose_keep(df_recent5, fallback=top_n)
            df_recent5 = df_recent5[df_recent5["name"].isin(keep5)]
            df_base5 = df_base5[df_base5["name"].isin(keep5)]

            df_delta_v5 = (
                df_recent5[["name", "vol_share"]]
                .merge(
                    df_base5[["name", "vol_share"]],
                    on="name",
                    how="outer",
                    suffixes=("_recent5", "_base5"),
                )
                .fillna(0)
            )
            df_delta_v5["pp_change"] = (
                df_delta_v5["vol_share_recent5"] - df_delta_v5["vol_share_base5"]
            )
            df_delta_v5 = df_delta_v5.sort_values("pp_change", ascending=False)
            df_delta_v5["name"] = pd.Categorical(
                df_delta_v5["name"],
                categories=df_delta_v5["name"].tolist(),
                ordered=True,
            )

            fig_vol_std5 = px.bar(
                df_delta_v5,
                x="name",
                y="pp_change",
                color=(df_delta_v5["pp_change"] > 0),
                color_discrete_map={True: "green", False: "red"},
                title=f"{selected_sector} — Volume Share Δ — Recent 5d vs Days 6–10 (ending {recent5[0]})",
            )
        fig_vol_std5.update_layout(
            xaxis_title="Stock",
            yaxis_title="Change in volume share (pp)",
            height=400,
            showlegend=False,
        )
        st.plotly_chart(fig_vol_std5, use_container_width=True)
    else:
        st.info("Not enough history for 5d vs 6–10 volume view.")

    conn.close()

elif page == "📊 Comparison Insights":
    st.subheader("📊 Comparison of 30-Day vs 10-Day Trends")

    try:
        # Load the CSV generated by intel_comparator.py
        df_compare = pd.read_csv("intel_comparison_report.csv")

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
        SELECT * FROM signals
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
            FROM signals
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
        SELECT * FROM signals
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
            FROM signals
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
