from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
from pymongo import MongoClient, ASCENDING, UpdateOne

from helper import parse_tickers  # reuse your existing helper

load_dotenv()  # load .env if present

# ------------------- CONFIG -------------------

TZ = ZoneInfo(os.getenv("TZ", "America/Toronto"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "stockdb")

TICKERS = parse_tickers(
    os.getenv("TICKERS"),
    default=["BCE.TO", "BNS.TO", "CM.TO", "CSH-UN.TO", "ENB.TO", "FTS.TO"],
)

DAYS_5M = 60

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("fetch_5m_history")

print(f"Using tickers: {TICKERS}")

# ------------------- MONGO --------------------

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# New collection: raw 5-minute bars
px_5m_raw = db["PriceFor5MinuteInterval"]

# Unique index so reruns just upsert/overwrite, not duplicate
px_5m_raw.create_index(
    [("ticker", ASCENDING), ("ts", ASCENDING)],
    unique=True,
)


# ------------------- DOWNLOAD -----------------

def download_5m(ticker: str, days: int = DAYS_5M) -> pd.DataFrame:
    """
    Download last `days` of 5-minute data for a ticker from yfinance.
    """
    period = f"{min(days, 60)}d"  # yfinance limit for 5m is ~60 days
    log.info("%s: downloading 5m(%s)", ticker, period)

    df = yf.download(
        ticker,
        period=period,
        interval="5m",
        auto_adjust=False,
        progress=False,
        group_by="column",
    )

    if df is None or df.empty:
        log.warning("%s: no 5m data returned", ticker)
        return pd.DataFrame()
    
    # yfinance often returns MultiIndex columns like ('Adj Close', 'BCE.TO')
    if isinstance(df.columns, pd.MultiIndex):
        # keep only the first level: 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume'
        df.columns = [str(col[0]) for col in df.columns]

    # Make sure index is a tz-aware DatetimeIndex in UTC
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    df = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()]

    return df


# ------------------- UPSERT -------------------

def upsert_5m_raw(ticker: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        log.info("%s: nothing to upsert (empty DataFrame)", ticker)
        return 0

    created_at = datetime.now(timezone.utc)
    ops: list[UpdateOne] = []

    for ts, row in df.iterrows():
        # Force all keys to be strings        
        base = {str(k): v for k, v in row.to_dict().items()}

        base["ticker"] = ticker
        base["ts"] = ts.to_pydatetime()          # aware datetime, UTC
        base["isActive"] = True
        base["createDatetime"] = created_at

        ops.append(
            UpdateOne(
                {"ticker": ticker, "ts": base["ts"]},
                {"$set": base},
                upsert=True,
            )
        )

    if not ops:
        log.info("%s: no ops generated for upsert", ticker)
        return 0

    result = px_5m_raw.bulk_write(ops, ordered=False)
    upserted = (result.upserted_count or 0) + (result.modified_count or 0)
    log.info("%s: upserted/modified %d 5m bars", ticker, upserted)
    return upserted


# ------------------- PIPELINE -----------------

def ingest_ticker_5m(ticker: str):
    """
    Fetch last 60 days of 5m data for one ticker and save to Mongo.
    """
    try:
        df_5m = download_5m(ticker, DAYS_5M)
        # print(df_5m.head())
        n = upsert_5m_raw(ticker, df_5m)
        log.info("%s: finished ingest, total rows processed: %d", ticker, n)
    except Exception as e:
        log.exception("%s: failed ingest: %s", ticker, e)


def run_all(tickers=None):
    tickers = tickers or TICKERS
    for t in tickers:
        ingest_ticker_5m(t)


# ------------------- MAIN ---------------------

if __name__ == "__main__":
    run_all(TICKERS)
