# institutional_watch.py

import sqlite3
import pandas as pd
from datetime import datetime

# === CONFIG ===
DB_PATH = "data/ngx_equities.db"
STEALTH_LOOKBACK_DAYS = 14
MIN_BUIDUP_DAYS = 5
MIN_ZONE_DAYS = 8

# ✅ Optional: Limit to known mid/large cap stocks (e.g., NGX30)
preferred_stocks = set([
    "ZENITHBANK", "GTCO", "ACCESSCORP", "UBA", "STANBIC", "FBNH", "MTNN", "BUACEMENT",
    "DANGCEM", "SEPLAT", "NESTLE", "NB", "GUINNESS", "WAPCO", "FLOURMILL", "DANGSUGAR",
    "INTBREW", "GEREGU", "ETI", "PZ", "CUSTODIAN", "UCAP", "STERLINGNG", "FIDELITYBK",
    "TRANSCORP", "NASCON", "CADBURY", "UNILEVER", "BERGER", "OANDO", "JAIZBANK",
    "WEMABANK", "UNITYBNK", "AIICO", "SOVRENINS", "MANSARD", "NEM", "AFRIPRUD"
    "HONYFLOUR", "MAYBAKER", "VITAFOAM", "CHAMPION", "IKEJAHOTEL", "JOHNHOLT", "TIP",
    "TRANSCORP HOTELS", "UACN", "BUAFOODS", "CORNERST", "LIVESTOCK", "UPDCREIT",
    "NAHCO", "MCNICHOLS", "ETERNA", "FIDSON", "FCMB", "OKOMUOIL", "CWG", "PRESCO"
    
])

# === LOAD DATA ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
conn.close()

df["date"] = pd.to_datetime(df["date"])
cutoff = df["date"].max() - pd.Timedelta(days=STEALTH_LOOKBACK_DAYS)
df = df[df["date"] >= cutoff].copy()
df = df.sort_values(by=["name", "date"])

# === DETECT STEALTH BEHAVIOR ===
def detect_stealth(group):
    group["avg_volume_10"] = group["volume"].rolling(window=10, min_periods=1).mean()
    group["avg_trades_10"] = group["trades"].rolling(window=10, min_periods=1).mean()
    group["abs_change"] = group["change"].abs()

    group["stealth_day"] = (
        (group["volume"] > 1.1 * group["avg_volume_10"]) &
        (group["trades"] > 1.1 * group["avg_trades_10"]) &
        (group["abs_change"] < 2.0)
    )


    return group

df = df.groupby("name", group_keys=False).apply(detect_stealth)
df = df.reset_index(drop=True)




# === SCORE AND SUMMARIZE ===
summary = (
    df.groupby("name")
    .agg(
        stealth_days=("stealth_day", "sum"),
        avg_volume_14=("volume", "mean"),
        avg_change_14=("change", "mean"),
        last_close=("close", "last")
    )
    .reset_index()
)

# 🔍 Filter to mid/large caps only
summary = summary[summary["name"].isin(preferred_stocks)]

# 🚦 Assign Institutional Status
def assign_zone(n):
    if n >= MIN_ZONE_DAYS:
        return "🔒 Institutional Zone"
    elif n >= MIN_BUIDUP_DAYS:
        return "⚠️ Buildup Detected"
    else:
        return None

summary["zone"] = summary["stealth_days"].apply(assign_zone)
summary = summary[summary["zone"].notnull()].copy()

# ✅ Final Format
summary["date_generated"] = datetime.today().strftime("%Y-%m-%d")
summary = summary.sort_values(by="stealth_days", ascending=False)

# === SAVE TO DB ===
conn = sqlite3.connect(DB_PATH)
conn.execute("DROP TABLE IF EXISTS institutional_watch")
summary.to_sql("institutional_watch", conn, index=False)
conn.close()
