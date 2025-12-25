import sqlite3
import pandas as pd

DB_PATH = "data/ngx_equities.db"

def estimate_buy_sell_volume(df):
    df = df.sort_values(by=["name", "date"]).copy()
    df["prev_close"] = df.groupby("name")["close"].shift(1)
    df["prev_vol"] = df.groupby("name")["volume"].shift(1)

    def calc_ratio(row):
        if pd.isna(row["prev_close"]) or pd.isna(row["prev_vol"]) or row["volume"] == 0:
            return None
        if row["close"] > row["prev_close"]:
            net_buy = row["volume"] - row["prev_vol"]
            base_buy = 0.5 * row["prev_vol"]
            buy_volume = max(0, net_buy + base_buy)
            return round(buy_volume / row["volume"], 3)
        elif row["close"] < row["prev_close"]:
            net_sell = row["volume"] - row["prev_vol"]
            base_sell = 0.5 * row["prev_vol"]
            sell_volume = max(0, net_sell + base_sell)
            return round(1 - (sell_volume / row["volume"]), 3)
        else:
            return 0.5

    df["buy_ratio"] = df.apply(calc_ratio, axis=1)
    return df

# === RUNNER ===
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM equities", conn)

# Apply to all companies
df_with_ratio = estimate_buy_sell_volume(df)

# Option 1: Save as a new table
df_with_ratio.to_sql("buy_ratio_signals", conn, if_exists="replace", index=False)

# Option 2: Merge into signals table if you already use one
# df_signals = pd.read_sql("SELECT * FROM signals", conn)
# merged = df_signals.merge(df_with_ratio[["name", "date", "buy_ratio"]], on=["name", "date"], how="left")
# merged.to_sql("signals", conn, if_exists="replace", index=False)

conn.close()
print("✅ Buy ratios calculated and saved.")
