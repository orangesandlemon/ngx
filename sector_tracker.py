# sector_tracker.py
import pandas as pd
import sqlite3
from datetime import datetime
from sector_map import sector_map  # This must be a dict: { 'ZENITHBANK': 'Banking', ... }

# === DB Config ===
DB_PATH = "data/ngx_equities.db"

def main():
    # === Load Latest Day's Data ===
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM equities", conn)
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    latest_date = df['date'].max()
    df_today = df[df['date'] == latest_date].copy()

    # === Map sectors ===
    df_today['sector'] = df_today['name'].map(sector_map)
    df_today = df_today.dropna(subset=['sector'])

    # === Calculate metrics ===
    grouped = df_today.groupby('sector')
    sector_data = []

    for sector, group in grouped:
        avg_change = group['change'].mean()
        total_volume = group['volume'].sum()
        total_trades = group['trades'].sum()

        # Identify top mover (by % change * volume)
        group['impact_score'] = group['change'].abs() * group['volume']
        top_mover = group.sort_values('impact_score', ascending=False).iloc[0]['name']

        sector_data.append({
            'date': latest_date.strftime("%Y-%m-%d"),
            'sector': sector,
            'avg_change_pct': round(avg_change, 2),
            'total_volume': int(total_volume),
            'total_trades': int(total_trades),
            'top_mover': top_mover
        })

    df_sector = pd.DataFrame(sector_data)
    df_sector = df_sector.sort_values(by='avg_change_pct', ascending=False)

    df_sector.to_csv("sector_performance.csv", index=False)
    print("✅ sector_performance.csv generated.")

if __name__ == "__main__":
    main()
