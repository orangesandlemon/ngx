import yfinance as yf
import pandas as pd

tickers = [  # abbreviated list; paste your full list here
    "AAK.ST", "ABB.ST", "ACAD.ST", "ADDT-B.ST", "AFRY.ST", "ALFA.ST", "ALIF-B.ST", "ALIG.ST", "ALLEI.ST",
    "ALLIGO-B.ST", "ALIV-SDB.ST", "AMBEA.ST", "ANOD-B.ST", "APOTEA.ST", "AQ.ST", "ARJO-B.ST", "ASKER.ST", "ASMDEE-B.ST", "ASSA-B.ST",
    "ATCO-B.ST", "ATRLJ-B.ST", "ATT.ST", "AXFO.ST", "AZA.ST", "AZN.ST", "BAHN-B.ST", "BALD-B.ST", "BEIA-B.ST", "BEIJ-B.ST",
    "BERG-B.ST", "BETS-B.ST", "BILI-A.ST", "BILL.ST", "BIOA-B.ST", "BIOG-B.ST", "BIOT.ST", "BOL.ST", "BONEX.ST", "BOOZT.ST",
    "BRAV.ST", "BUFAB.ST", "BURE.ST", "CAMX.ST", "CAST.ST", "CATE.ST", "CIBUS.ST", "CLAS-B.ST", "CORE-A.ST", "CRED-A.ST",
    "CS.ST", "DIOS.ST", "DOM.ST", "DYVOX.ST", "EKTA-B.ST", "ELUX-A.ST", "ELUX-B.ST", "EMBRAC-B.ST", "EMIL-B.ST", "ENGCON-B.ST",
    "EPI-A.ST", "EPRO-B.ST", "EQT.ST", "ERIC-A.ST", "ERIC-B.ST", "ESSITY-A.ST", "ESSITY-B.ST", "EVO.ST", "FABG.ST", "FAG.ST",
    "FNOX.ST", "FOI-B.ST", "FPAR-A.ST", "GETI-B.ST", "GOTL-A.ST", "GRNG.ST", "HEBA-B.ST", "HEM.ST", "HEXA-B.ST", "HM-B.ST",
    "HMS.ST", "HOFI.ST", "HOLM-A.ST", "HPOL-B.ST", "HTRO.ST", "HUFV-A.ST", "HUSQ-A.ST", "INDT.ST", "INDU-A.ST", "INSTAL.ST",
    "INTEA-B.ST", "INVE-A.ST", "INVE-B.ST", "INWI.ST", "IPCO.ST", "ITAB.ST", "IVSO.ST", "JM.ST", "KAR.ST", "KINV-A.ST",
    "KINV-B.ST", "LAGR-B.ST", "LATO-B.ST", "LIAB.ST", "LIFCO-B.ST", "LIME.ST", "LOOMIS.ST", "LUND-B.ST", "MCAP.ST", "MCOV-B.ST",
    "MEKO.ST", "MILDEF.ST", "MIPS.ST", "MMGR-B.ST", "MTG-A.ST", "MTRS.ST", "MYCR.ST", "NCC-A.ST", "NCAB.ST", "NDA-SE.ST",
    "NEWA-B.ST", "NIBE-B.ST", "NMAN.ST", "NOLA-B.ST", "NORION.ST", "NORVA.ST", "NP3.ST", "NYF.ST", "OEM-B.ST", "ORES.ST",
    "OSSD.ST", "PDX.ST", "PEAB-B.ST", "PLAZ-B.ST", "PLEJD.ST", "PNDX-B.ST", "RATO-A.ST", "RAY-B.ST", "RESURS.ST", "ROKO-B.ST",
    "RUSTA.ST", "SAAB-B.ST", "SAGA-A.ST", "SAND.ST", "SAVE.ST", "SBB-B.ST", "SCA-A.ST", "SCA-B.ST", "SCST.ST", "SDIP-B.ST",
    "SEB-A.ST", "SECARE.ST", "SECT-B.ST", "SECU-B.ST", "SHB-A.ST", "SHOT.ST", "SINCH.ST", "SKA-B.ST", "SKF-A.ST", "SKF-B.ST",
    "SKIS-B.ST", "SLP-B.ST", "SOBI.ST", "SPLTN.ST", "SSAB-A.ST", "STEF-B.ST", "STOR-B.ST", "STORY-B.ST", "SUS.ST", "SVEAF.ST",
    "SVOL-A.ST", "SWEC-A.ST", "SWED-A.ST", "SYNSAM.ST", "SYSR.ST", "TEL2-B.ST", "TELIA.ST", "TFBANK.ST", "THULE.ST",
    "TREL-B.ST", "TROAX.ST", "TRUE-B.ST", "VBG-B.ST", "VER.ST", "VIMIAN.ST", "VIT-B.ST", "VITR.ST", "VOLCAR-B.ST", "VOLO.ST",
    "VOLV-A.ST", "VOLV-B.ST", "WALL-B.ST", "WIHL.ST", "XANO-B.ST", "XVIVO.ST", "YUBICO.ST", "ZZ-B.ST", "ADDV-B.ST"

]

results = []
for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        print(info)
        market_cap = info.get("marketCap", None)
        results.append({"Ticker": ticker, "MarketCap": market_cap})
    except Exception:
        results.append({"Ticker": ticker, "MarketCap": None})

df = pd.DataFrame(results)
df.to_csv("swedish_stocks_marketcap.csv", index=False)
print("✅ Saved to swedish_stocks_marketcap.csv")
