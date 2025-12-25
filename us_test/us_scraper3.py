# === File: sweden/scraper_yahoo.py ===

import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import os

# === CONFIG ===
DB_PATH = "data/us_equities.db"
TICKERS = [
"ATDS","SONG","MECPF","MXUBY","MANA","DTZNY","TDGGF","SLDE","GRLMF","ONWRY","MOGMF","VROYF","BORMF","GEHDF","NRYCF","NRUC","ETCG","GJP","GHRTF","GJH","GBAT","ODOT","RSHGY","PYT","GLIV","TSLVF","DDHLY","CABI","ELC","ARKB","FGDL","BNO","DEFI","XSIAX","CPPBY","IPOD","FXB","BTC","XNJJY","TFSA","KTN","KTH","LPCHY","NCIQ","USL","BRRR","HAPVD","GJO","MGPFY","CGEH","OSOL","GJT","OBNB","CUPPF","GLNK","CTBB","GJS","TKMTY","GLDM","IPB","JBS","BUHPY","BTCO","EZET","CTATF","GSOL","GYGLF","GJR","FOMTF","EGG","GLTR","FXC","LICYQ","ETI-P","ZLME","ZCSH","FOFA","ZRCN","DOGP","HODL","MGTE","EZBC","OAK-PA","AAAU","ETHE","UNL","CETH","VAPE","ETHV","PDSRX","PPLT","MGNO","SIVR","IAUM","NHPAP","WYGC","ENGCQ","TEAD","XPTFX","BAR","GXLM","SGOL","FBTC","BITW","UUP","GOEVQ","SVIX","FXF","NOVAQ","ICR-PA","IPCX","BHIC","BTCW","QVCD","OUNZ","PLTM","PALL","HBANP","UDN","ETHW","BITB","EZPZ","CHAR","HZEN","QETH","SEATW","FXA","TPTA","DNMRQ","BGXXQ","EHSI","BMYMP","USB-PA","BRK-A","TSMWF","JPM-PD","ASMLF","BML-PG","BABAF","BML-PH","BAC-PE","BML-PJ","BML-PL","BAC-PB","BAC-PK","NVSEF","TOYOF","AZNCF","PCCYF","RYDAF","HBCYF","WFC-PL","WFC-PY","SNEJF","MBFJF","EADSF","TBB","TTFNF","SNYNF","CILJF","UNLYF","BUDFF","BHPLF","WFC-PC","BCDRF","BPAQF","RTPPF","ABLZF","ABBNY","BTAFF","IDEXF","BYDDF","NCRRP","NTTYY","SMFNF","AIQUF","CFRHF","STOHF","NETTF","SNPMF","MBGAF","GLAXF","USB-PH","EBBNF","GS-PA","BNPQF","CTA-PB","BAESF","GS-PD","PPRUF","AMXOF","GLCNF","FMCCT","PBR-A","MS-PI","KDDIF","MS-PF","MS-PE","RLXXF","DGEAF","DUK-PA","EIPAF","CCZ","BAMXF","BYMOF","IFNNF","MZHOF","HTHIF","NGGTF","SCHW-PD","TKPHF","DKILF","JDCMF","BECEF","SPG-PJ","INGVF","MET-PA","TRPCF","MET-PE","LLDTF","PLDGP","WOPEF","BBVXF","HLNCF","DNZOF","TCANF","PSA-PH","HNDAF","BSQKZ","SVNDF","PUKPF","BAMGF","MRAAF","HEI-A","TCKRF","ALL-PH","CRARF","ALL-PB","RBSPF","FNCTF","PSA-PK","BAIDF","LEN-B","CIXPF","BCLYF","NOKBF","ERIXF","RSMDF","CODGF","LAAOF","STMEF","ORXCF","CAJPY","TEFOF","BBDO","FITB","GNMSF","VODPF","ANNSF","DLR-PK","RCIAF","FCNCB","FWONK","FWONA","CJPRF","AMSYF","SAXPF","RYAOF","FOX","MKC-V","TNRSF","RSTRF","HBAN","CAJFF","DLR-PJ","YAHOF","SOJC","COCSF","TELNF","STT-PG","PGPEF","WLMIF","EJPRF","FWONB","XPNGF","HIG-PG","FNMAS","DUKH","POAHF","RKLIF","NRSCF","HRNNF","OMVJF","Z","EBR-B","JHIUF","SPXSF","NWS","FNMAJ","RYLPF","AMCCF","FAXXF","JBSAY","CHEAF","CHKIF","ADTTF","KEY-PK","SOAGY","FNMAH","TEVJF","SSMXF","HUNGF","KEY-PJ","JBARF","SGIOF","FRFFF","EMRAF","NLY-PG","PTPIF","HKHGF","APO-PA","NLY-PF","KEY-PI","FNGD","GFIOF","FRFXF","NCSYF","BF-B","SVYSF","WSO-B","FMCQF","MAA-PI","LBRDB","RF-PC","SNNUF","LBRDK","BZLFF","ALMMF","ICHGF","ACGLO","ERRAF","UHAL-B","TAP-A","PBNNF","WPPGF","NNGPF","AOMFF","GPAEF","KIKOF","AEGOF","CUKPF","FREJO","YAMHF","NDEKF","ASRMF","RDEIF","JGSHF","LBTYB","UEPCN","CXMSF","OUKPF","PSORF","AGNCN","AGNCM","RZB","UEPEP","UEPEO","PARAA","PSNYW","SASOF","VNORP","ROHCF","GS-PC","FMCCI","FRT-PC","OSCUF","NIOIF","AMH-PH","VNO-PL","FMCKI","UEPCP","AMH-PG","VNO-PM","ATH-PA","RKUNF","UEPEM","UEPEN","VOYA-PB","GIFLF","PADEF","GIFOF","THNPY","JCYCF","DSECF","CMSA","RNR-PF","KIM-PM","CIG-C","FANUF","KIM-PL","BCUCF","SLG-PI","CWEN-A","KAKKF","FUPEF","GIKLY","QBCRF","OAK-PB","PCG-PA","QBCAF","UNMA","SLFPF","CSWI","BPOPO","PCG-PB","LIFX","SBYSF","HGMCF","PHTCF","HOYFF","ATGFF","PCG-PD","FNMFN","AFRAF","PCG-PH","TKAYF","PCG-PG","GAERF","AXS-PE","DTW","DFRYF","SR-PA","SLMBP","ROYMY","RUSHB","REXR-PB","THNPF","SF-PB","WTFCM","TWO-PC","JTKWY","AKO-B","CIM-PB","CIM-PD","BHFAP","MOG-B","ESGRP","MYTHF","GEF-B","TWO-PB","TWO-PA","WRB-PE","WBS-PF","ESGRO","VLYPO","RLJ-PA","PEB-PF","PEB-PE","UA","ROYMF","PEGRF","GNGYF","PTXAF","ASB-PE","HMDCF","SOCGP","AILLI","HOVNP","BATRB","WELPP","AILLN","TRTN-PA","CDGLF","FNMAN","TROLB","FMCCJ","FNMAI","SRG-PA","NHNKF","CODQL","AILLO","LXP-PC","TKCM","AGM-A","FNMAT","FNMAK","ORAAF","CENTA","FNMAL","AILLP","IVR-PC","WLYB","FNMAO","FNMFO","ALTB","AAUCF","AAUCD","GDV-PH","GRPFF","PLYA","NGL-PB","BFS-PD","FMCKL","PMT-PA","PMT-PB","LILAB","STSFF","GAB-PH","HCXY","FMCCS","MHLA","ATROB","FMCCO","AHL-PD","CODI-PB","NYMTN","FMCKN","FREJN","CMGMF","FMCKO","FMCKJ","GLOP-PB","FMCKM","GLOP-PC","BELFB","LILAK","FREGP","GLOP-PA","FMCCH","FMCCK","HL-PB","FMCCP","FMCCM","AHH-PA","PLLTL","KBSR","IIPR-PA","INN-PE","GAM-PB","CODI-PA","SPLP","GTN-A","DGICB","MEOBF","DRDGF","KELYB","CMRE-PB","CMRE-PD","CMRE-PC","HLTC","HOVVB","HVT-A","GGT-PE","GAMI","GGN-PB","DADA","ARTNB","BRCNF","UMH-PD","CUBI-PE","ECCX","CUBI-PF","MITT-PA","NCV-PA","NEWTI","PTCHF","MITT-PB","OXLCO","CRD-B","AIRTP","GMRE-PA","WBHC","LANDM","SENEB","SEAT","THPTF","GAINL","AVHHL","VSOGF","CIO-PA","NHHS","GUT-PC","CKDXF","CNTHP","IHRTB","CNLPL","NCZ-PA","DFPH","DSX-PB","CNTHO","CHMI-PB","ERLFF","BROG","BHR-PD","HAWLN","AHT-PD","AHT-PG","PRRUF","CNTHN","SLNCF","HAWEN","BHR-PB","OB","CNLHO","APLMW","CPSR","CNLHP","TCMFF","ARCXF","CNLTP","FGPRB","CNLTL","IPHYF","CNPWM","PRTHU","CHMI-PA","CNPWP","LANDP","CNLPM","RVRF","AHT-PH","CNLTN","AHT-PF","AHT-PI","GDRZF","ENO","CORBF","GSCCF","CNLHN","BNIGF","IVEVF","AONC","SB-PC","NVNXF","EBRCZ","WONDF","IMRA","PMHG","DLNG-PA","MOYFF","CDR-PC","CDR-PB","MEHCQ","BBXIB","TDACU","ECF-PA","RDIB","FATBB","SOHOO","BCV-PA","RGBPP","GLU-PB","GNTLF","RLFTF","ZHYBF","GSL-PB","BCTF","DLNG-PB","ARRKF","MPVDF","BKSC","SOHOB","SOHON","SSHT","CULL","EAXR","TCBC","CPTP","GRFXY","SBBCF","UBOH","MTMV","OCGSF","GFASY","AMJT","ZIVO","GRFXF","MDNAF","BCOW","LCHD","SMTSF","RAASY","CIZN","NZEOF","FBIOP","BBXIA","CHTH","MSVB","AATC","AVLNF","IDWM","UONEK","WSKEF","GBNY","HBUV","YELLQ","BIOE","DSHK","NSRCF","ENDI","WVVIP","INFT","USNU","ARBKF","IMUC","PWCO","WHLM","SFES","CATG","CYCCP","WHLRD","PMDI","GBCS","ATAO","SPTY","PRNAF","WBSR","FZMD","VMNT","TVE","MCLE","BNSOF","TVC","CUBT","MYCB","WINSF","EVTK","SWISF","VQSSF","CEAD","FULO","NEXCF","SHVLF","HGLD","SITS","FBDS","FALC","SILEF","UCASU","EGLXF","BRRN","MVCO","GLUC","SIPN","JETR","EBZT","UCIX","CBDS","CMOT","WDLF","ATIP","TGCB","RBCN","PMEDF","RSCF","CRTD","LICYF","TPHS","AMTY","MTTCF","CANN","AMMX","HALB","VCNX","GEMZ","SMKG","FTRS","ACRL","SNNC","CETXP","DTGI","EGTYF","AAGH","OMGAQ","SRMX","GRDAF","AXIM","JRSS","ELRA","GPFT","TKOI","SVVC","SNWR","AGNPF","PWDY","PCYN","PAANF","HMMR","MGON","RDAR","IGPK","WHLRP","BDRL","TNBI","CTHR","RTON","TUPBQ","VNUE","IMPM","CGAC","GYST","KGKG","WNFT","SONX","SYRS","GMZP","TLSS","MGHL","EHVVF","ELST","IWAL","CMGO","TSOI","TLIF","GMBL","FKST","MMMW","INNI","TKMO","PHBI","CBGL","FLXT","USDP","SNPW","GYGC","XELA","CBNT","NTRR","APSI","BKGM","SHGI","ENDV","CDSG","PLPL","EDGM","KITL","KEGS","MASN","DPUI","PIKM","EESH","ECOX","SMFL","WTII","BMXC","FAVO","GIPL","AFIB","AIMI","DWAY","RTSL","GXXM","FOMI","ECXJ","SINC","RNGC","ASPU","NXGB","BZRD","WTER","BAC-PL","BAC-PM","PCG-PX","GRABW","BAC-PN","BAC-PO","BAC-PP","BAC-PQ","BACRP","SSSSL","AILIO","AILIP","RF-PE","RF-PF","WTFCP","WTFCN","STXYF","SAT","SAY","SAZ","SAJ","RPDL","CIMO","CIMN","LARAX","ISRLU","NRSAX","FACTU","ATMP","BWVTF","COWTF","DJP","GBUG","VXZ","TAPR","VXX","GRN","JJCTF","JJETF","JJGTF","PGMFF","GSRTU","GSRTR","NPPXF","TLSIW","PSPX","SNNRF","SNRBY","GIGGU","CCCXU","HNIT","FCELB","BDRY","BWET","SPKLU","KMPB","COLAU","MSPRW","LPBBW","LPBBU","OACCU","KVACU","ASPCU","KATXD","TACHU","FSHPU","UYSCU","MSEXP","KHOB","AAVXF","FORFF","FTRSF","MLMC","CNOBP","FROPX","TBMCR","MRKY","AACT-UN","TANAF","UCB-PI","USB-PS","USB-PQ","USB-PR","TRTX-PC","NEWTH","NEWTG","CNVEF","EQV-UN","SCE-PM","SCE-PG","SCE-PJ","SCE-PK","SCE-PL","SCE-PN","PORTU","BULLZ","BULLW","ARQQW","EXEEZ","EXEEW","TRTN-PB","TRTN-PC","ISMCF","TRTN-PD","TRTN-PE","TRTN-PF","HMLPF","PMTRU","CPER","RITM-PA","RITM-PC","RITM-PD","AGQ","BOIL","EUO","GLL","KOLD","YCL","YCS","ZSL","UGL","ULE","UVXY","VIXM","VIXY","SCO","SVXY","ATGAF","ATGPF","RRAUF","RRAWF","RAC-UN","FNMAP","QVCGP","QVCGB","WLLBW","MNSBP","FCNCO","FCNCP","NICHX","CCIRU","EGHAU","GSHRU","YOTAU","NTRBW","BHFAM","BHFAO","DNQUF","NWSAL","CUBWU","TRWD","DRH-PA","SSRGF","SHO-PH","SHO-PI","ABR-PD","ABR-PF","AIFEU","HBANM","EQH-PC","MBNKP","MBNKO","RWAYL","RWAYZ","NLSPW","PLMUF","LENDX","SQFTP","SREA","CBRRF","BRQL","CNFRZ","SCHW-PJ","ECCC","ECC-PD","ECCF","ECCU","ECCW","BAMKF","BKAMF","BKFAF","BKFPF","ACAT","AIZN","BKFDF","BRPSF","BNH","BNJ","BROXF","BXDIF","DSHKN","DSHKP","BCEFF","BCEPF","BCEXF","BCEIF","BCPPF","OXSQG","OXSQZ","RIV-PA","GDSTU","SOJD","SOJE","SOJF","ELPC","INTEU","POWWP","APOS","HWM-P","KCHVU","PMT-PC","CFR-PB","PMTU","PMTW","PMTV","NGHI","ATEKU","CSWCZ","RBMCF","RYLBF","NNDNF","CHARU","BTSGU","FRBP","ALCYU","EVGOW","ASCIX","TDS-PU","TDS-PV","HFRO-PA","HFRO-PB","CSTUF","HWLDF","PNFPP","SIGIP","VNO-PN","VNO-PO","MYPSW","TRINI","TRPPF","TRPEF","TCENF","WELPM","LBRDP","WCC-PA","HCIIP","MNQFF","MNUFF","SZZLU","IVAUF","OPINL","TRINZ","RANGU","OAKUR","OAKUU","MITN","MITP","BXNCP","ALDFU","TBNRL","TDBCP","TDBKF","RWTN","RWTP","RWTO","WENNU","AGM-PE","AGM-PF","AGM-PG","PSEC-PA","SYF-PA","SYF-PB","HHEGF","PMFAX","NPACU","FHN-PB","FHN-PC","FHN-PE","FHN-PF","PTNRF","GLLIU","CGCTU","AIMTF","GDV-PK","GTENU","GEGGL","HPE-PC","FGMCU","FIISO","CCIXU","PELIU","LCCCU","SOCGM","UVIX","FSEN","FOSLL","BANC-PF","FITBO","FITBP","WHLRL","OTRKP","LIANY","SNTUF","MLCMF","DJTWW","ATMCU","QSEAU","OXLCP","OXLCZ","OXLCG","OXLCI","OXLCL","OXLCN","XCAPX","NXDT-PA","MNESP","FCRX","DLR-PL","LEVWQ","FLG-PU","FLG-PA","PBNAF","PMBPF","PMMBF","PPLAF","EVTWF","EOSEW","NSA-PB","REGCO","REGCP","RFAIU","WEIBF","RNR-PG","WHFCL","RILYG","RILYK","RILYL","RILYN","RILYP","RILYZ","KARX","ANG-PD","ANG-PB","RSVRW","WLSS","ALFUU","ICRP","ENBFF","CCNEP","EBGEF","EBRGF","EBRZF","STRF","STRK","STRD","ENBOF","ENBNF","ENBHF","ENBMF","ENBGF","ENBRF","ENNPF","IPCXU","HTFB","RPT-PC","ATCO-PD","ATCO-PH","MACIU","NHICU","QVCC","GNL-PB","GNL-PE","GNL-PD","LEGT-UN","JVSAU","FATBP","C-PN","HTOOW","HYMCL","MBINM","MBINN","PNMXO","MBINL","PREJF","WTGUU","EICA","EICC","EICB","REXR-PC","NWPND","ATH-PB","ATH-PD","TLPPF","GAINN","GAINZ","GAINI","TAVIU","FVNNU","FVNNR","GNT-PA","UMBFP","PRIF-PD","PRIF-PF","PRIF-PJ","PRIF-PK","PRIF-PL","HPKEW","BOWNU","FAXRF","JACS-UN","DYNXU","GHBWF","CCLFX","DHCNI","FREJP","TFLM","FMCKK","LFMDP","MSBIP","GLP-PB","MSSUF","MET-PF","TACOU","GIGGF","GGT-PG","VIASP","WFC-PD","WFCNP","WFC-PA","WFC-PZ","JPM-PJ"
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