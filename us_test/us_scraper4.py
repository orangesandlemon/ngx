# === File: sweden/scraper_yahoo.py ===

import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import os

# === CONFIG ===
DB_PATH = "data/us_equities.db"
TICKERS = [
"STBXF", "EMED", "INND","SPIEF","CIRX","VEVMQ","FSTJ","HWNI","HGYN","CNBX","NUVOQ","DTB","DTG","STSR","GMBLP","SDHIU","AOMD","AOMN","HONDU","AFJKU","ACR-PC","ACR-PD","NTWOU","BNKD","BNKU","BULZ","JETD","CARD","CARU","WTID","WTIU","JETU","NRGU","OILD","OILU","SHNY","FNGO","FNGS","GDXD","GDXU","DULL","BUHPF","IHICF","NAMMW","GECCI","GECCH","GECCO","GRWTF","GWLPF","GRWLF","MRCIF","CUYTF","DMXCF","MBUMF","YMHAY","PACHU","TOKCF","SMSOF","YKLTF","SEPSF","IDPUF","THQQF","LVCE","SNIRF","SNROF","PRXXF","BAKR","CCOOD","IDKOF","MEIUF","CHWRF","GDERF","GFSAY","SAABF","CYJBF","VTTGF","RMXI","SYYYF","CITAF","SKLTF","NBRWF","HROWL","HROWM","BEAGU","DMYY-UN","CGBDL","TMSOF","SVIIU","DATSW","HWCPZ","VCRRX","CANE","CORN","SOYB","WEAT","KFIIU","FOUR-PA","NLY-PI","UNOV","ASAIY",
"PEB-PG","PEB-PH","IMAQU","YHNAU","CCIA","CUBB","CCLDO","DX-PC","EIOAX","RDACU","DBRG-PI","DBRG-PJ","RMHI","DBVTF","RNWWW","MCAGU","FTAIM","FTAIN","YSHLF","IPODU","EERGF","LABFF","CPNNF","SMTGF","KOKSF","NPEHF","JENA-UN","NELR","BEPH","BEPI","BEP-PA","BRENF","BEPJ","UEPCO","LLOBF","MNYWW","GENVR","TCRG","KREF-PA","UZF","UZD","UZE","DTSQU","XJNGF","PSBAF","MICLD","NULGD","FLNCF","NAMSW","NZEOY","VMCUF","BIP-PA","BIPJ","WTCHF","FILG","MINR","BKLPF","NETDU","FKURF","MANDF","SHMZF","ARZTD","RKWAD","NTTDF","VYRE","MGPUF","HPHTF","AERGP","GLCP","WSBCP","LIXTW","HYAC-UN","NMFCZ","RAJAF","RIBBU","AACIU","TMCWW","HSPTU","AUB-PA","AGQPF","AQNB","MDV-PA","FORLU","SBBTF","SBSNF","ADC-PA","HSPOU","COF-PI","COF-PJ","COF-PK","COF-PL","COF-PN","WBS-PG","SPMA","SACH-PA","SCCC","SCCD","SCCE","SCCF","SCCG",
"ARGD","ASB-PF","ASBA","CSDX","WOLTF","OPP-PA","OPP-PB","OPP-PC","TFINP","PMCUF","JBHIF","CBRA","AHICF","VRBCF","SYAXF","IPSOF","ONWRF","PNDZF","DTZZF","BCKIF","TVAIU","UUGWF","KRNGF","LNXSF","MDDNF","PUIGF","DSMFF","SNPTF","NXFNF","MGHTF","OSSFF","DPLMF","SGBAF","LOKVU","RTACW","RTACU","AIBRF","NCHEF","INPAP","CPPTL","ALTG-PA","NMPGY","NMPRY","ZTOEF","SHLAF","SHLRF","BSAAU","ADYYF","PCPPF","BYCBF","FCHRF","CRAQU","IRRHF","SUNXF","SDZXF","FERAU","MIBE","NEE-PS","NEE-PT","NEE-PR","KOBNF","BLFBY","BAFBF","NWSZF","SPGDF","ASBRF","RDPTF","SCSKF","DGMDF","CADE-PA","MONRF","MLSPF","BYDIF","LSHGF","REECF","AXINU","EBCOF","MIMTF","OUTFF","TYNPF","RSNHF","NXPRF","CRSLF","XALCX","ARRNF","USCTF","DPLS","PTZH","IDAI","CREV","PHIL","RHEP","AZTR","BAOS","BRQSF","BLNC","GARWF","NRIS","XBIO","YERBF",
"MOVE","CMGHF","DRMA","LMRMF","PLRZ","BSLK","SGBX","GLTO","LPTV","XELB","NRRWF","SGLY","CNSP","PTNT","JAGX","YJGJ","GLVT","SBEV","BKYI","APTOF","CMND","STEK","NROM","EUEMF","REVFF","SIGY","LBSR","ECIA","CTNT","CHEK","MTNB","EVOK","AVCRF","RAPH","VIVS","ACON","EVTV","FHLD","LCFY","DGLY","SILO","CNTGF","UGRO","CASK","AWHL","QWTR","TTOO","VEST","VSBGF","OVTZ","TRLEF","PSYCF","SNRG","XXII","SRCO","BUDZ","JWEL","RDHL","TNLX","ROYL","BOXL","TOVX","SMX","FMTO","ATXG","VTAK","CPMV","IVP","RECHF","PW","LUVU","VYCO","HSCS","SLE","HAMRF","SGN","PRFX","NAAS","HAO","VRAX","ASRE","TRGED","IMTE","GLMD","NBY","RELI","BITTF","KANT","CGDXF","OCTO","REVB","FWFW","KWE","FECOF","CCOOF","XYLB","AEMD","CNNN","ALTX","MYSZ",
"AVPMF","VERO","ORMNF","THAR","IWSH","MITI","TOFB","ILUS","FUNI","TGL","MAGE","SBIG","AHRO","XRTX","PKTX","ADMT","ITP","TIVC","WLDS","FFNTF","CETX","AGRI","ALCE","PALI","SXTP","MGAM","ATCH","PUCCF","XPON","GRI","ADIL","IPDN","PCSA","XONI","WTO","VPRB","ZVSA","AQB","IDXG","BPTSY","AAUGF","INM","ASTI","AKAN","PEXZF","ADTX","KNOS","SPRB","NGTF","MRIN","LIXT","IMIMF","APVO","CYAN","DFLI","ELAB","PTIX","IVF","DHCC","OHCS","MBYMF","VRRCF","TANH","AUUD","NUVI","HSTA","QLGN","ALDS","CNRCF","GCTK","VIVC","ALDA","UOKA","CPMD","ALZN","SCPX","VPER","ISPC","XAGE","SENR","ZPHYF","PBM","AIXN","AUMN","TC","NDRA","EDXC","APDN","ISGIF","BJDX","PNST","KCRD","DMNIF","MSTH","BDCC","ENTO","BRWC","QIND","ABQQ","VSTTF",
"SGD","IDVV","WOLV","LGMK","GIDMF","UK","SCNI","CMLS","NVSGF","FAMI","FOXO","INKW","UPC","CDT","SEAV","LBUY","GPLB","IBRLF","UUSAF","CMHSF","BLMZ","OWPC","ESMC","WINT","GRTX","OZSC","USAQ","SRGZ","INTV","TNFA","ATMH","BPTH","PNYG","FRCB","SVRE","MMVVF","CHKKF","ATVK","OCEA","OVATF","NXUR","LITSD","BSAI","PGOL","RSLS","DBMM","QRON","IPTNF","EEGI","EGMCF","TLLTF","VISM","WORX","NIVF","GEGP","BRVO","WHLR","BLIS","GETR","AFMDQ","VYST","MULN","CAPC","AGTX","BBBMF","ILST","LEBGF","LGHL","MRNJ","PITEF","WLGS","PRSI","NGCG","GMER","PBFFF","SHPH","PTOS","NSTM","KAYS","ICCT","SVUHF","GRLF","SLRX","HHHEF","YHC","DXF","THURF","RGBP","OILCF","RAKR","TOGI","BSPK","ROVMD","SMCE","CAMG","PBLA","GCEHQ","PAPLF","STKH","CBDY",
"NUWE","CBMJ","MMND","DREM","MMEX","EVFM","GNTOF","EMBYF","EMMA","MRPT","SNAX","EAWD","ODYY","FRZT","ENSV","DYNT","KRFG","HLLK","SYRA","ISCO","IPSI","SKVI","PTPI","MNTR","CCCFF","CLDVF","LINMF","ARRT","CONC","TRVN","HEPA","BRST","HWKE","LQWC","TCBPY","HGAS","AVRW","BSFC","ENRT","ETUGF","FBCD","LKCOF","LADX","BEGI","CBDL","RTCJF","NRHI","OMQS","NCNA","VEII","VIRX","CCCP","REOS","BOTY","MDCE","OMTK","VNTH","PTCO","ATXI","LFLY","CLOW","SYGCF","CBDW","QSJC","GFMH","YBCN","SKFG","GMPW","ITOX","STQN","SRSG","NBND","ASII","CSUI","MLRT","GSAC","EVLLF","SSOK","NNAX","SKYI","BDPT","CWPE","SYIN","NIHK","CYTOF","MCOM","BIOLQ","USLG","SLDC","DTII","GSLR","GCAN","NXEN","NMHI","BLPG","SAML","SPQS","ZAPPF","CLEUF","JNCCF",
"QPRC","VHAI","LYTHF","RMTG","MTLK","VINC","JKSM","PNXP","LEEN","WDDD","GRVE","FCHS","YCRM","VRPX","NCNCF","HSDT","QTZM","PLTYF","BHILQ","DGWR","LUXH","AOXY","CTKYY","SATT","NBBI","LOWLF","ECPL","QTTOY","BRAXF","BACK","AXDXQ","GRPS","SPOWF","CWNOF","KRBPQ","SING","JPM-PK","JPM-PL","JPM-PM","VYLD","AMJB","AMUB","BDCX","BDCZ","CEFD","USML","UCIB","MVRL","PFFL","QULL","SCDL","SMHB","IWML","MLPB","MLPR","MTUL","HDLB","IFED","IWDL","CICB","LNC-PD","ACGLN","XXAAU","EMICF","T-PC","RDAGU","T-PA","HMELF","IMPPP","CTTRF","MACT","CIVII","PDYNW","IINNW","CLBR-UN","AMPGW","ATMVU","WTMAU","DCOMP","DCOMG","DDT","OKMN","EMCGU","WAL-PA","AGNCL","TPGXL","AGNCO","AGNCP","INN-PF","NVNBW","CHSCL","CHSCM","CHSCN","CHSCO","NMKCP","NMKBP","NMPWP","NEWEN","RCIAX","WAFDP",
"WYTC","VFLEX","BW-PA","BWSN","ZIONP","SSST","SPHIF","RCD","SLFQF","SLFIF","SUNFF","AFGD","AFGE","AFGB","RHEPA","SVCCU","MGR","MGRE","MGRB","MGRD","GDLG","CODI-PC","VFSWW","FCUL","SPNT-PB","CADCX","BA-PA","AFBL","PDPA","NVAAF","ALB-PA","FGBIP","DMAAU","MS-PO","MS-PP","MS-PL","RENXF","NSARO","NSARP","JXN-PA","JETBF","LMMY","AMLIF","LSEB","CDAUF","CRBD","SLMUF","ALL-PI","ALL-PJ","TRLC","JWSWF","JWSUF","VLDXW","SWKHL","JOCM","EGUVF","EGSVF","DAAQU","CSLUF","GOODN","GOODO","SIMAU","TWOD","VCMIX","GTLS-PB","UHGI","RAAQU","EPR-PC","EPR-PE","SENEM","SENEL","RCB","RC-PC","RC-PE","BPOPM","MBAVU","POLEU","PCTTU","AACBU","IGTAU","EFSCP","REEUF","LPAAU","NYMTZ","NYMTL","NYMTM","NYMTI","NYMTG","SBXD-UN","LFT-PA","WALDW","HNNAZ","BUSEP","LLYVA","LLYVB","QETAU","LOCLW","TSLTF","GAB-PK","OBTC",
"VACHU","ATIIU","CDZIP","AEAEU","CRTUF","PDSKX","BBLGW","CNDAW","CNDAU","SRRIX","TRSO","TFC-PO","TFC-PR","CMSC","CMSD","CMS-PC","MAYAU","FULTP","IROHU","GREEL","MDCXW","BYNOU","CCCMU","CCCMW","AEFC","GMTH","TVACU","TSPH","KKR-PD","KKRT","LTAFX","LTCFX","KEY-PL","CTO-PA","BOH-PA","BOH-PB","ONBPO","ONBPP","BPYPM","BPYPN","BPYPO","CAPNU","COBA","ATLCL","ATLCP","ATLCZ","LANDO","PGYWW","FGSN","ARR-PC","CLDT-PA","TCBIO","OWSCX","TETUF","FGFPP","DGP","DGZ","DZZ","OLOXF","CGABL","VSSYW","MHNC","NUKKW","SNV-PE","NOEMU","ITOR","PSA-PI","PSA-PJ","PSA-PR","PSA-PS","PSA-PL","PSA-PM","PSA-PN","PSA-PO","PSA-PP","PSA-PQ","PSA-PF","PSA-PG","FBRT-PE","HVIIU","GPATU","NWOEF","STRRP","IRRXU","CHPGU","MTB-PJ","MTB-PH","XOMAO","XOMAP","OFSSH","MLACU","SEAL-PA","SEAL-PB","NFTM","CTLPP","VCICU","LUXHP","ABLLL","SPE-PC","SOUL-UN",
"GSCE","FRSPF","MGSD","CFG-PE","CFG-PH","ARBKL","JAGL","SUACU","GLTK","BRETF","NCLTF","SHPPF","TYHOF","HAWLI","HAWLL","HAWLM","HAWEM","ELCPF","FLMNF","NGHLF","TEN-PE","TEN-PF","PAASF","AHL-PF","LTESF","ET-PI","MFA-PC","MFAN","MFAO","METCL","METCZ","METCB","PFXNZ","KIM-PN","ANKM","PLMKU","CCGWW","ARES-PB","SF-PC","SF-PD","ABAKF","NEMCL","ISPOW","OCCIN","OCCIO","OCCIM","DYCQU","DTLAP","EFC-PA","EFC-PB","EFC-PC","EFC-PD","BFS-PE","GLDI","SLVO","USOI","MNYFF","CTDD","FUPPF","SSPPF","WEBJF","TKMEF","BTGRF","DNACF","EBOSF","HNSDF","SMFRF","EDVGF","ARHUF","HAIUF","SLNHP","CHNEY","ECX","CTO","STRW","DNTH","LIND","CURLF","NUVB","MOFG","MTUS","NWTN","TEN","SANA","NESR","KIND","PRAA","SNCY","URGN","MHD","SPFI","EMBC","FSBC","LAC","ASGI","HTBK","ABL","RXST","NMCO","ALNT",
"MUJ","BFLY","BCYC","REAL","VVR","NGL","PEO","CLB","QSG","EUBG","SGML","BUI","UHT","HKD","AVNS","THRY","METC","AHG","AUTL","RGLS","ALT","RLAY","CCO","KURA","CASS","USNA","MYE","YRD","IIM","NKX","CAPR","CCAP","VLGEA","GUYGF","AMRK","IAUX","GDOT","PBT","HOV","NBR","CSTL","OPAL","TYRA","GHY","CURV","DSU","MMU","VERV","MNMD","FPI","MCS","BORR","SMBK","BXC","DJCO","CCD","EGBN","OLP","PSNL","TBPH","BGB","ALRS","MLAB","RSRBF","KRT","MSC","BLW","CLFD","DCTH","FTHY","CBNK","DOMO","WASH","KIDS","LEGH","PUBM","NXTT","SCVL","WIW","SFIX","AVK","DNA","BTBT","EIM","VALN","ASM","HZO","FRPH","GBFH","AGS","SAFX","ODP","MUX","HNST","NML","IVR","KIO","TNGX","FISI","VKQ",
"ACIC","GHM","CGEM","DIAX","GCI","GUT","SCHL","EBTC","EHAB","GUG","ATEX","NCMI","APEI","CPAC","IQI","RZLV","APPS","FUFU","AXL","SHBI","VCV","ARKO","PML","FMNB","EVTL","LYTS","IPI","MDRX","STK","AMLX","HONE","BLDP","CLMB","NRIM","INNV","IVA","QD","CRD-A","PGC","CEVA","TALK","ZKH","FGOVF","PFIS","FTK","CYRB","CION","EBF","BWMN","WDH","RSVR","BCAL","ABVX","NLOP","CHCT","LAES","EQV","BLE","PRTC","BRELY","TTI","PKST","BITF","ADV","BME","ORKA","MYGN","TREE","YORW","SBC","MAGN","AUNA","ANSC","NXJ","SWBI","NATH","RGNX","CODI","ATYR","TYG","CCNE","PSTL","KODK","PSBD","TSSI","NBB","ISD","AAM","GBLI","DPG","BALY","GNTY","DDL","SIGA","NPFD","AMPX","FRA","KE","CELC","CWCO",
"DNUT","IGD","RPAY","CLW","HPS","NPB","LDI","PGEN","HOUS","CYH","SEPN","AXGN","GTN","ATAI","GRVY","DC","MITK","TITN","RMT","BVS","MNRO","INRE","UNTY","CNDT","PERI","BOC","MREO","BNTC","CHW","VHI","ANGO","ETO","AERG","CYDY","MBAV","BACQ","EBFI","CCRN","WNC","CMCO","TRC","OFIX","XFLT","ZBIO","CCSI","DDI","CCCM","ACNB","FFWM","MCI","COFS","CAL","ATGL","DXYZ","AROW","THW","HPI","CMTG","NETD","PTHRF","GAMB","CTKB","CLNE","VENU","ERAS","OBE","TCBX","RIOFF","SHYF","OPEN","RTAC","DFP","ETB","SRDX","CRMT","BFK","NMAI","CMPS","DIN","ZEO","BAND","DBO","TLRY","BWB","LSEA","SGU","EU","NQP","RAPP","KELYA","DOGZ","ASC","EVN","KRNY","EAD","SDHY","EGY","TECX","TGE","SCGX",
"KMDA","AURA","XPOF","FFA","FBIZ","MTW","KFII","SKYT","FFIC","SB","GLASF","LIMN","DH","GPRK","SVC","GBAB","ORGO","GROY","GAU","MUA","DHIL","SMLR","NWBO","BIGC","SCM","CLCO","CCIX","TACO","GRRR","LAB","NWPX","NNNN","CPS","RRBI","ITOS","DFDV","UPXI","HBCP","ITIC","LND","ETON","LWAY","SVRA","GPAT","ALF","VCIC","HIX","BSRR","CMCL","MVF","SLP","SENS","CARE","BYON","CEP","REEMF","TRAK","GCBC","NUS","SLQT","BNED","JMIA","SAR","PTHL","KFS","PFL","BSVN","ANNA","HQL","HIO","RIGL","IBEX","CRNC","VIGL","NRC","HUMA","LAND","FULC","RZLT","VKI","ADCT","OTLY","ISOU","LEO","TMCI","VALU","MYN","ACTG","MSBI","XPER","OM","VBNK","CLDT","SPMC","BMRC","PACB","JRI","BGS","RICK","SDHI"
"JACK","ABSI","UAMY","GFR","TBRG","KULR","PPT","URG","SLI","SES","SOR","MATV","MBX","GILT","CWBC","POET","EFR","ARTNA","NAN","CEPT","GPRE","TACH","CDRO","GDEV","KTF","",
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equities_full (
            name TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (name, date)
        )
    """)

# === Main Loop ===
all_data = []

for ticker in TICKERS:
    try:
        print(f"[+] Downloading {ticker} from Yahoo Finance...")
        df = yf.download(ticker, start=start_date, end=end_date, interval="1d", auto_adjust=True)

        if df.empty:
            with open("failed_tickers.txt", "a") as f:
                    f.write(f"{ticker}\n")
            print(f"[!] No data for {ticker}")
            continue
    except Exception:
        with open("failed_tickers.txt", "a") as f:
            f.write(f"{ticker}\n")
        continue

    df.reset_index(inplace=True)
    df["name"] = ticker
    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = df[["name", "date", "Open", "High", "Low", "Close", "Volume"]]
    df.columns = ["name", "date", "open", "high", "low", "close", "volume"]
    df = df.dropna()

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