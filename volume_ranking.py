import sqlite3
import pandas as pd


def compute_accumulation_signals_3days(db_path="data/ngx_equities.db"):
    conn = sqlite3.connect(db_path)

    # Step 1: Get last 3 available trading dates (non-weekend safe)
    last_3_dates_query = """
        SELECT DISTINCT date FROM equities
        WHERE volume IS NOT NULL AND value IS NOT NULL AND close IS NOT NULL
        ORDER BY date DESC
        LIMIT 3
    """
    last_3_dates = [row[0] for row in conn.execute(last_3_dates_query).fetchall()]
    if len(last_3_dates) < 3:
        raise ValueError("Need at least 3 days of data in the equities table.")

    # Step 2: Load relevant data for those 3 dates
    placeholders = ",".join("?" for _ in last_3_dates)
    data_query = f"""
        SELECT date, name, volume, value, trades, close AS price, sub_sector
        FROM equities
        WHERE date IN ({placeholders})
    """
    df = pd.read_sql_query(data_query, conn, params=last_3_dates, parse_dates=["date"])

    # Step 3: Pivot into wide format
    df_wide = df.pivot(
        index="name", columns="date", values=["volume", "value", "price"]
    )
    df_wide.columns = [f"{col[0]}_{col[1].date()}" for col in df_wide.columns]
    df_wide = df_wide.fillna(0).reset_index()

    # Step 4: Sort dates for detection logic
    sorted_dates = sorted(last_3_dates, reverse=True)
    d1, d2, d3 = sorted_dates[0], sorted_dates[1], sorted_dates[2]

    # Step 4.5: Add total columns
    df_wide["volume_total"] = (
        df_wide[f"volume_{d3}"] + df_wide[f"volume_{d2}"] + df_wide[f"volume_{d1}"]
    )
    df_wide["value_total"] = (
        df_wide[f"value_{d3}"] + df_wide[f"value_{d2}"] + df_wide[f"value_{d1}"]
    )

    # Step 5: Detect accumulation/distribution/neutral
    def detect_signal(row):
        v = [row[f"volume_{d3}"], row[f"volume_{d2}"], row[f"volume_{d1}"]]
        val = [row[f"value_{d3}"], row[f"value_{d2}"], row[f"value_{d1}"]]
        p = [row[f"price_{d3}"], row[f"price_{d2}"], row[f"price_{d1}"]]

        if v[0] < v[1] < v[2] and val[0] < val[1] < val[2] and p[0] <= p[1] <= p[2]:
            return "accumulation"

        # Distribution type 1: volume/value down, price down
        elif v[0] > v[1] > v[2] and val[0] > val[1] > val[2] and p[0] >= p[1] >= p[2]:
            return "distribution"

        # Distribution type 2: volume/value up, price down
        elif v[0] < v[1] < v[2] and p[0] >= p[1] >= p[2]:
            return "distribution"

        else:
            return "neutral"

    df_wide["signal"] = df_wide.apply(detect_signal, axis=1)

    # Step 6: Add latest contextual info (price, value, trades, subsector)
    latest_info = df[df["date"] == sorted_dates[0]][
        ["name", "price", "value", "trades", "sub_sector"]
    ].drop_duplicates("name")
    df_final = df_wide.merge(latest_info, on="name", how="left")

    # ✅ OPTIONAL: Reorder columns for cleaner display
    cols = df_final.columns.tolist()
    # Example: move volume_total and value_total to right after the 3-day volume/value cols
    # You can customize this as needed
    ordered_cols = []
    # Start with name
    ordered_cols.append("name")

    # Add all volume_* columns
    ordered_cols += [
        col for col in cols if col.startswith("volume_") and "total" not in col
    ]
    # Then total volume
    if "volume_total" in cols:
        ordered_cols.append("volume_total")

    # Add all value_* columns
    ordered_cols += [
        col for col in cols if col.startswith("value_") and "total" not in col
    ]
    # Then total value
    if "value_total" in cols:
        ordered_cols.append("value_total")

    # Add price_* columns
    ordered_cols += [col for col in cols if col.startswith("price_")]

    # Add signal and remaining meta info
    for col in ["signal", "price", "value", "trades", "sub_sector"]:
        if col in cols:
            ordered_cols.append(col)

    # Apply reorder
    df_final = df_final[ordered_cols]

    # Step 7: Save result to table
    df_final.to_sql("accumulation_signals_3day", conn, if_exists="replace", index=False)

    conn.close()
    print(
        f"✅ Written to table 'accumulation_signals_3day' for dates: {d1}, {d2}, {d3}"
    )
    return df_final


# Run function
# df = compute_accumulation_signals_3days()
compute_accumulation_signals_3days()
