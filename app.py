# app.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

st.set_page_config(page_title="NGX Signal Dashboard", layout="wide")

DB_PATH = "data/ngx_equities.db"

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
        return ['background-color: #fff7e6'] * len(row)  # Light gold
    else:
        return [''] * len(row)


# === Filters ===
st.title("📈 NGX Signal Tracker")
st.caption("Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))

col1, col2, col3 = st.columns(3)

with col1:
    selected_signal = st.selectbox("Filter by Signal Type", ["All"] + sorted(df['signal'].unique().tolist()))

with col2:
    selected_action = st.selectbox("Filter by Action", ["All"] + sorted(df['action'].unique().tolist()))

with col3:
    selected_date = st.selectbox("Filter by Date", ["All"] + sorted(df['date'].unique().tolist(), reverse=True))

# === Apply filters ===
filtered_df = df.copy()

if selected_signal != "All":
    filtered_df = filtered_df[filtered_df["signal"] == selected_signal]

if selected_action != "All":
    filtered_df = filtered_df[filtered_df["action"] == selected_action]

if selected_date != "All":
    filtered_df = filtered_df[filtered_df["date"] == selected_date]

# === Display ===
st.subheader(f"📊 Showing {len(filtered_df)} signals")

if st.button("🔄 Refresh Signals"):
    st.cache_data.clear()
    st.rerun()

st.dataframe(
    filtered_df[["name", "date", "signal", "confidence_score", "action", "buy_range", "explanation"]]
    .style.apply(highlight_row, axis=1),
    use_container_width=True
)

