# test_streamlit_app.py
import streamlit as st
import sqlite3
import pandas as pd
import os

st.title("NGX Equity Tracker Test")

db_path = "data/ngx_equities.db"

if not os.path.exists(db_path):
    st.warning("Database not found. Make sure you've created 'data/ngx_equities.db'.")
else:
    conn = sqlite3.connect(db_path)
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql(query, conn)
    conn.close()
    st.write("Tables in database:")
    st.dataframe(tables)
