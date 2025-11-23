from dataclasses import dataclass


# Official NSE master lists
NSE_EQUITY_CSV = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
# ETF: try legacy + api fallbacks — pages change occasionally
NSE_ETF_CSV_FALLBACKS = [
    "https://nsearchives.nseindia.com/content/equities/eq_etfseclist.csv",
    "https://nsearchives.nseindia.com/content/equities/eq_etfseclist.csv?download=true",
]


# BSE sources (ETF/Equity master). BSE doesn't have a stable public CSV endpoint;
# we keep placeholders to map when you get a stable URL or file drop.
BSE_EQUITY_MASTER_CSV = None # e.g., path to downloaded Scrip Master CSV
BSE_ETF_LIST_CSV = None # e.g., path to a curated CSV of BSE ETFs


# AMFI scheme master — for index funds & all mutual funds
AMFI_SCHEME_MASTER = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0"


@dataclass
class UniverseDoc:
    symbol: str
    name: str
    exchange: str
    source_fields: dict