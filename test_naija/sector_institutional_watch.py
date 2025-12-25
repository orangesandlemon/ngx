# sector_institutional.py

import pandas as pd
import sqlite3
from datetime import datetime
from sector_map import sector_map

# === CONFIG ===
DB_PATH = "data/ngx_equities.db"
STEALTH_LOOKBACK_DAYS = 14
MIN_BUIDUP_DAYS = 5
MIN_ZONE_DAYS = 8

preferred_stocks = set([
    "ZENITHBANK", "GTCO", "ACCESSCORP", "UBA", "STANBIC", "FBNH", "MTNN", "BUACEMENT",
    "DANGCEM", "SEPLAT", "NESTLE", "NB", "GUINNESS", "WAPCO", "FLOURMILL", "DANGSUGAR",
    "INTBREW", "GEREGU", "ETI", "PZ", "CUSTODIAN", "UCAP", "STERLINGNG", "FIDELITYBK",
    "TRANSCORP", "NASCON", "CADBURY", "UNILEVER", "BERGER", "OANDO", "JAIZBANK",
    "WEMABANK", "UNITYBNK", "AIICO", "SOVRENINS", "MANSARD", "NEM", "AFRIPRUD",
    "HONYFLOUR", "MAYBAKER", "VITAFOAM", "CHAMPION", "IKEJAHOTEL", "JOHNHOLT", "TIP",
    "TRANSCORP HOTELS", "UACN", "BUAFOODS", "CORNERST", "LIVESTOCK", "UPDCREIT",
    "NAHCO", "MCNICHOLS", "ETERNA", "FIDSON", "FCMB", "OKOMUOIL", "CWG", "PRESCO"
])

# === LOAD DATA ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)
df["date"] = pd.to_datetime(df["date"])
conn.close()

cutoff = df["date"].max() - pd.Timedelta(days=STEALTH_LOOKBACK_DAYS)
df = df[df["date"] >= cutoff].copy()
df = df[df["name"].isin(preferred_stocks)]
df["sector"] = df["name"].map(sector_map).fillna("Other")
df = df.sort_values(by=["name", "date"])

# === DETECT STEALTH ===
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

# === SUMMARIZE PER STOCK ===
stock_summary = (
    df.groupby(["name", "sector"])
    .agg(
        stealth_days=("stealth_day", "sum"),
    )
    .reset_index()
)

# === ASSIGN ZONES ===
stock_summary["zone"] = stock_summary["stealth_days"].apply(
    lambda x: "🔒 Institutional Zone" if x >= MIN_ZONE_DAYS else (
        "⚠️ Buildup Detected" if x >= MIN_BUIDUP_DAYS else None
    )
)
stock_summary = stock_summary[stock_summary["zone"].notnull()]

# === AGGREGATE BY SECTOR ===
sector_summary = (
    stock_summary.groupby("sector")
    .agg(
        institutional_zone_count=("zone", lambda x: sum(z == "🔒 Institutional Zone" for z in x)),
        buildup_count=("zone", lambda x: sum(z == "⚠️ Buildup Detected" for z in x)),
        institutional_zone_stocks=("name", lambda x: ", ".join(stock_summary.loc[x.index][stock_summary["zone"] == "🔒 Institutional Zone"]["name"])),
        buildup_stocks=("name", lambda x: ", ".join(stock_summary.loc[x.index][stock_summary["zone"] == "⚠️ Buildup Detected"]["name"]))
    )
).reset_index()

sector_summary["date_generated"] = datetime.today().strftime("%Y-%m-%d")

# === SAVE TO DB ===
conn = sqlite3.connect(DB_PATH)
conn.execute("DROP TABLE IF EXISTS sector_stealth_summary")
sector_summary.to_sql("sector_stealth_summary", conn, index=False)
conn.close()

print("✅ sector_stealth_summary table updated in DB.")
