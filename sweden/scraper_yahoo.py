# === File: sweden/scraper_yahoo.py ===

import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import os

# === CONFIG ===
DB_PATH = "data/omx_equities.db"
TICKERS = [
    "XACT-OMXS30.ST",
    "AAK.ST",
    "ABB.ST",
    "ACAD.ST",
    "ADDT-B.ST",
    "AFRY.ST",
    "ALFA.ST",
    "ALIF-B.ST",
    "ALIG.ST",
    "ALLEI.ST",
    "ALLIGO-B.ST",
    "ALIV-SDB.ST",
    "AMBEA.ST",
    "ANOD-B.ST",
    "APOTEA.ST",
    "AQ.ST",
    "ARJO-B.ST",
    "ASKER.ST",
    "ASMDEE-B.ST",
    "ASSA-B.ST",
    "ATCO-B.ST",
    "ATRLJ-B.ST",
    "ATT.ST",
    "AXFO.ST",
    "AZA.ST",
    "AZN.ST",
    "BAHN-B.ST",
    "BALD-B.ST",
    "BEIA-B.ST",
    "BEIJ-B.ST",
    "BERG-B.ST",
    "BETS-B.ST",
    "BILI-A.ST",
    "BILL.ST",
    "BIOA-B.ST",
    "BIOG-B.ST",
    "BIOT.ST",
    "BOL.ST",
    "BONEX.ST",
    "BOOZT.ST",
    "BRAV.ST",
    "BUFAB.ST",
    "BURE.ST",
    "CAMX.ST",
    "CAST.ST",
    "CATE.ST",
    "CIBUS.ST",
    "CLAS-B.ST",
    "CORE-A.ST",
    "CRED-A.ST",
    "CS.ST",
    "DIOS.ST",
    "DOM.ST",
    "DYVOX.ST",
    "EKTA-B.ST",
    "ELUX-A.ST",
    "ELUX-B.ST",
    "EMBRAC-B.ST",
    "EMIL-B.ST",
    "ENGCON-B.ST",
    "EPI-A.ST",
    "EPRO-B.ST",
    "EQT.ST",
    "ERIC-A.ST",
    "ERIC-B.ST",
    "ESSITY-A.ST",
    "ESSITY-B.ST",
    "EVO.ST",
    "FABG.ST",
    "FAG.ST",
    "FNOX.ST",
    "FOI-B.ST",
    "FPAR-A.ST",
    "GETI-B.ST",
    "GOTL-A.ST",
    "GRNG.ST",
    "HEBA-B.ST",
    "HEM.ST",
    "HEXA-B.ST",
    "HM-B.ST",
    "HMS.ST",
    "HOFI.ST",
    "HOLM-A.ST",
    "HPOL-B.ST",
    "HTRO.ST",
    "HUFV-A.ST",
    "HUSQ-A.ST",
    "INDT.ST",
    "INDU-A.ST",
    "INSTAL.ST",
    "INTEA-B.ST",
    "INVE-A.ST",
    "INVE-B.ST",
    "INWI.ST",
    "IPCO.ST",
    "ITAB.ST",
    "IVSO.ST",
    "JM.ST",
    "KAR.ST",
    "KINV-A.ST",
    "KINV-B.ST",
    "LAGR-B.ST",
    "LATO-B.ST",
    "LIAB.ST",
    "LIFCO-B.ST",
    "LIME.ST",
    "LOOMIS.ST",
    "LUND-B.ST",
    "MCAP.ST",
    "MCOV-B.ST",
    "MEKO.ST",
    "MILDEF.ST",
    "MIPS.ST",
    "MMGR-B.ST",
    "MTG-A.ST",
    "MTRS.ST",
    "MYCR.ST",
    "NCC-A.ST",
    "NCAB.ST",
    "NDA-SE.ST",
    "NEWA-B.ST",
    "NIBE-B.ST",
    "NMAN.ST",
    "NOLA-B.ST",
    "NORION.ST",
    "NP3.ST",
    "NYF.ST",
    "OEM-B.ST",
    "ORES.ST",
    "OSSD.ST",
    "PDX.ST",
    "PEAB-B.ST",
    "PLAZ-B.ST",
    "PLEJD.ST",
    "PNDX-B.ST",
    "RATO-A.ST",
    "RAY-B.ST",
    "RESURS.ST",
    "ROKO-B.ST",
    "RUSTA.ST",
    "SAAB-B.ST",
    "SAGA-A.ST",
    "SAND.ST",
    "SAVE.ST",
    "SBB-B.ST",
    "SCA-A.ST",
    "SCA-B.ST",
    "SCST.ST",
    "SDIP-B.ST",
    "SEB-A.ST",
    "SECARE.ST",
    "SECT-B.ST",
    "SECU-B.ST",
    "SHB-A.ST",
    "SHOT.ST",
    "SINCH.ST",
    "SKA-B.ST",
    "SKF-A.ST",
    "SKF-B.ST",
    "SKIS-B.ST",
    "SLP-B.ST",
    "SOBI.ST",
    "SSAB-A.ST",
    "STEF-B.ST",
    "STOR-B.ST",
    "STORY-B.ST",
    "SUS.ST",
    "SVEAF.ST",
    "SVOL-A.ST",
    "SWEC-A.ST",
    "SWED-A.ST",
    "SYNSAM.ST",
    "SYSR.ST",
    "TEL2-B.ST",
    "TELIA.ST",
    "TFBANK.ST",
    "THULE.ST",
    "TREL-B.ST",
    "TROAX.ST",
    "TRUE-B.ST",
    "VBG-B.ST",
    "VER.ST",
    "VIMIAN.ST",
    "VIT-B.ST",
    "VITR.ST",
    "VOLCAR-B.ST",
    "VOLO.ST",
    "VOLV-A.ST",
    "VOLV-B.ST",
    "WALL-B.ST",
    "WIHL.ST",
    "XANO-B.ST",
    "XVIVO.ST",
    "YUBICO.ST",
    "ZZ-B.ST",
    "ADDV-B.ST",
]


# === Get Last Valid Trading Day ===
def get_last_trading_day():
    today = datetime.today()
    if today.weekday() == 5:  # Saturday
        return today - timedelta(days=1)  # Friday
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)  # Friday
    else:
        return today


last_trading_day = get_last_trading_day()
start_date = last_trading_day.strftime("%Y-%m-%d")
end_date = (last_trading_day + timedelta(days=1)).strftime("%Y-%m-%d")


# === Prepare DB ===
os.makedirs("sweden/data", exist_ok=True)
conn = sqlite3.connect(DB_PATH)

with conn:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS equities (
            name TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            market_cap INTEGER,
            PRIMARY KEY (name, date)
        )
    """
    )

# === Main Loop ===
all_data = []

for ticker in TICKERS:
    print(f"[+] Downloading {ticker} from Yahoo Finance...")
    df = yf.download(
        ticker, start=start_date, end=end_date, interval="1d", auto_adjust=True
    )

    try:
        market_cap = yf.Ticker(ticker).info.get("marketCap", None)
    except Exception:
        market_cap = None

    if df.empty:
        print(f"[!] No data for {ticker}")
        continue

    # Reset & sort
    df = df.reset_index().sort_values("Date")

    # Add metadata
    df["name"] = ticker
    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df["market_cap"] = market_cap

    # previous_close from original 'Close' (not yet renamed)
    df["previous_close"] = df["Close"].shift(1)

    # change_pct with safe divide
    df["change_pct"] = (
        (df["Close"] - df["previous_close"]) / df["previous_close"]
    ) * 100
    # If you prefer to keep first row with NaN previous_close, skip the drop below.
    # Optionally handle zeros:
    df.loc[df["previous_close"] == 0, "change_pct"] = None

    # Keep / rename columns (to lowercase)
    df = df[
        [
            "name",
            "date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "market_cap",
            "previous_close",
            "change_pct",
        ]
    ].rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    # Drop rows where price fields are missing; usually the first row per ticker will drop due to NaN previous_close
    df = df.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
            "volume",
            "previous_close",
            "change_pct",
        ]
    )

    all_data.append(df)


# === Insert to DB
if all_data:
    combined = pd.concat(all_data, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined = combined.drop_duplicates(subset=["name", "date"])

    with conn:
        # Delete just for this scraped date
        for d in combined["date"].unique():
            conn.execute("DELETE FROM equities WHERE date = ?", (d,))
        combined.to_sql("equities", conn, if_exists="append", index=False)

    print(f"[✓] Stored {len(combined)} total rows for {len(TICKERS)} tickers.")
    print(f"[✓] Inserted {len(combined)} rows into 'equities' table.")
    print(f"[✓] Done for {start_date}")
else:
    print("[x] No data to insert.")

conn.close()
