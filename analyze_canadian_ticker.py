from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
from pymongo import MongoClient, ASCENDING, UpdateOne

from helper import parse_tickers  # same helper you already have

load_dotenv()

# ------------------- CONFIG -------------------

TZ = ZoneInfo(os.getenv("TZ", "America/Toronto"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "stockdb")

TICKERS = parse_tickers(
    os.getenv("TICKERS"),
    default=["BCE.TO","BNS.TO"],
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("analyze_5m")

print(f"Using tickers: {TICKERS}")

# ------------------- MONGO --------------------

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

price_5m_col = db["PriceFor5MinuteInterval"]
daily_log_col = db["DailyLog"]
exec_state_col = db["EveryExecutionState"]

# ------------------- YFINANCE HELPERS --------------------

def flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns from yfinance into simple strings like 'Open', 'Close', etc."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(col[0]) for col in df.columns]
    return df


def download_yesterday_5m(ticker: str) -> pd.DataFrame:
    """
    Download 5-minute data for *yesterday only* for a ticker.

    Strategy:
      - Get last 1 days of 5m data from yfinance.
      - Convert index to UTC.
      - Filter rows whose local date (in TZ) == yesterday.
    """
    log.info("%s: downloading 5m for last 2 days", ticker)
    df = yf.download(
        ticker,
        period="2d",
        interval="5m",
        auto_adjust=False,
        progress=False,
        group_by="column",
    )

    if df is None or df.empty:
        log.warning("%s: no 5m data returned for last 2 days", ticker)
        return pd.DataFrame()

    df = flatten_yf_columns(df)

    # Ensure tz-aware UTC index
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    df = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()]

    # Filter only yesterday (in local TZ)
    now_local = datetime.now(TZ)
    yesterday_date = (now_local - timedelta(days=1)).date()

    local_dates = df.index.tz_convert(TZ).date
    mask = local_dates == yesterday_date
    df_yesterday = df.loc[mask]

    if df_yesterday.empty:
        log.warning("%s: no 5m bars for yesterday (%s)", ticker, yesterday_date)
    else:
        log.info(
            "%s: got %d 5m bars for yesterday (%s)",
            ticker, len(df_yesterday), yesterday_date,
        )

    return df_yesterday


def fetch_current_price(ticker: str) -> float | None:
    """
    Fetch current (or most recent) price for a ticker.

    Uses 1d of 1m data and returns the last non-NaN Close.
    """
    
    df = yf.download(
        ticker,
        period="1d",
        interval="1m",
        auto_adjust=False,
        progress=False,
        group_by="column",
    )

    if df is None or df.empty:
        log.warning("%s: no intraday data for current price", ticker)
        return None

    df = flatten_yf_columns(df)

    if "Close" not in df.columns:
        log.warning("%s: 'Close' column missing in current price data", ticker)
        return None

    closes = df["Close"].dropna()
    if closes.empty:
        log.warning("%s: no non-NaN Close values for current price", ticker)
        return None

    price = float(closes.iloc[-1])
    log.info("%s: current price = %f", ticker, price)
    return price


# ------------------- UPSERT YESTERDAY 5M --------------------

def upsert_5m_raw(ticker: str, df: pd.DataFrame) -> int:
    """
    Save 5-minute bars to MongoDB for a ticker.

    Fields per document:
      - ticker
      - ts (UTC datetime)
      - createDatetime (UTC datetime when we write)
      - plus all yfinance fields: Open, High, Low, Close, Adj Close, Volume, etc.
    """
    if df is None or df.empty:
        log.info("%s: nothing to upsert (empty DataFrame)", ticker)
        return 0

    created_at = datetime.now(timezone.utc)
    ops: list[UpdateOne] = []

    for ts, row in df.iterrows():
        base = {str(k): v for k, v in row.to_dict().items()}

        base["ticker"] = ticker
        base["ts"] = ts.to_pydatetime()  # aware UTC datetime
        base["createDatetime"] = created_at

        ops.append(
            UpdateOne(
                {"ticker": ticker, "ts": base["ts"]},
                {"$set": base},
                upsert=True,
            )
        )
    
    if not ops:
        log.info("%s: no ops generated", ticker)
        return 0

    result = price_5m_col.bulk_write(ops, ordered=False)
    upserted = (result.upserted_count or 0) + (result.modified_count or 0)
    log.info("%s: upserted/modified %d 5m bars", ticker, upserted)
    return upserted


def fetch_yesterday_for_all_tickers():
    """Fetch yesterday's 5m data for all tickers and store in PriceFor5MinuteInterval."""
    for t in TICKERS:
        try:
            df = download_yesterday_5m(t)
            upsert_5m_raw(t, df)
        except Exception as e:
            log.exception("%s: failed to fetch/upsert yesterday's data: %s", t, e)


# ------------------- DAILY LOG --------------------

DAILY_LOG_ID = "singleton"  # so we always have exactly one document


def get_last_update_local_date():
    doc = daily_log_col.find_one({"_id": DAILY_LOG_ID})
    if not doc or "lastUpdateDatetime" not in doc:
        return None

    dt = doc["lastUpdateDatetime"]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ).date()


def update_daily_log_now():
    now_utc = datetime.now(timezone.utc)
    daily_log_col.update_one(
        {"_id": DAILY_LOG_ID},
        {"$set": {"lastUpdateDatetime": now_utc}},
        upsert=True,
    )
    log.info("DailyLog updated with lastUpdateDatetime=%s", now_utc.isoformat())


def ensure_yesterday_data_if_needed():
    """
    If DailyLog.lastUpdateDatetime is not today (local) or missing:
      - fetch yesterday's 5m data for all tickers
      - update DailyLog to now
    """
    today_local = datetime.now(TZ).date()
    last_update_date = get_last_update_local_date()
    
    if last_update_date == today_local:
        log.info("DailyLog is up-to-date for today (%s), skipping fetch.", today_local)
        return
    
    log.info(
        "DailyLog lastUpdate=%s, today=%s -> fetching yesterday's 5m data.",
        last_update_date,
        today_local,
    )
    fetch_yesterday_for_all_tickers()
    update_daily_log_now()


# ------------------- STATS / ANALYSIS --------------------

def compute_stats_for_ticker(ticker: str) -> dict:
    """
    Compute:
      - avgClose30, avgClose90
      - minLow30, minLow90
    for the given ticker, using PriceFor5MinuteInterval.
    """
    now_utc = datetime.now(timezone.utc)
    cutoff_30 = now_utc - timedelta(days=30)
    cutoff_90 = now_utc - timedelta(days=90)

    # 30-day stats
    pipeline_30 = [
        {"$match": {"ticker": ticker, "ts": {"$gte": cutoff_30}}},
        {"$group": {
            "_id": None,
            "avgClose30": {"$avg": "$Close"},
            "minLow30": {"$min": "$Low"},
        }},
    ]

    res_30 = list(price_5m_col.aggregate(pipeline_30))
    avgClose30 = res_30[0]["avgClose30"] if res_30 else None
    minLow30 = res_30[0]["minLow30"] if res_30 else None

    # 90-day stats
    pipeline_90 = [
        {"$match": {"ticker": ticker, "ts": {"$gte": cutoff_90}}},
        {"$group": {
            "_id": None,
            "avgClose90": {"$avg": "$Close"},
            "minLow90": {"$min": "$Low"},
        }},
    ]

    res_90 = list(price_5m_col.aggregate(pipeline_90))
    avgClose90 = res_90[0]["avgClose90"] if res_90 else None
    minLow90 = res_90[0]["minLow90"] if res_90 else None

    return {
        "ticker": ticker,
        "avgClose30": avgClose30,
        "avgClose90": avgClose90,
        "minLow30": minLow30,
        "minLow90": minLow90,
    }


def analyze_and_store_execution_state():
    """
    For each ticker:
      - compute stats (avg/min for 30, 90 days)
      - fetch current price
      - perform comparisons:
          current < avgClose30
          current < avgClose90
          current < minLow30
          current < minLow90
          current < (avgClose30 - minLow30)
          current < (avgClose90 - minLow90)
      - store all tickers that satisfy each comparison into EveryExecutionState.
    """
    create_dt = datetime.now(timezone.utc)

    state_doc = {
        "createDatetime": create_dt,
        "lessThanAvg30": [],
        "lessThanAvg90": [],
        "lessThanMin30": [],
        "lessThanMin90": [],
        "lessThan80PctDiff30": [], # current < 0.8 * (avgClose30 - minLow30)
        "lessThan50PctDiff30": [], # current < 0.5 * (avgClose30 - minLow30)
        "lessThan80PctDiff90": [], # current < 0.8 * (avgClose90 - minLow90)
        "lessThan50PctDiff90": [], # current < 0.5 * (avgClose90 - minLow90)
    }

    for t in TICKERS:
        stats = compute_stats_for_ticker(t)
        avg30 = stats.get("avgClose30")
        avg90 = stats.get("avgClose90")
        min30 = stats.get("minLow30")
        min90 = stats.get("minLow90")

        # If any core stat is missing, skip comparisons for this ticker
        if avg30 is None or avg90 is None or min30 is None or min90 is None:
            log.warning("%s: insufficient stats, skipping comparisons", t)
            continue

        current_price = fetch_current_price(t)
        if current_price is None:
            log.warning("%s: no current price, skipping comparisons", t)
            continue

        # Each comparison: if current price is LESS than threshold, add to the relevant array
        def add_if_less(field_name: str, threshold: float):
            if threshold is None:
                return
            
            # print(f"{t}: comparing current {current_price} < threshold {threshold} for {field_name}")
            if current_price < threshold:
                state_doc[field_name].append({
                    "ticker": t,
                    "price": current_price,
                    "compareWith": float(threshold),
                })

        # current < avgClose30
        add_if_less("lessThanAvg30", avg30)

        # current < avgClose90
        add_if_less("lessThanAvg90", avg90)

        # current < minLow30
        add_if_less("lessThanMin30", min30)

        # current < minLow90
        add_if_less("lessThanMin90", min90)

        # 30-day diff-based thresholds
        diff30 = avg30 - min30
        if diff30 is not None and diff30 > 0:
            diffTo80FromAvg30 = avg30-(diff30 * 0.8)
            diffTo50FromAvg30 = avg30-(diff30 * 0.5)
            print(f"{t}: current_price = {current_price}, diff30 = {diff30} , avg30 = {avg30}, min30 = {min30}, diffTo80FromAvg30 = {diffTo80FromAvg30}, diffTo50FromAvg30 = {diffTo50FromAvg30}")
            # current < 80% of diff
            add_if_less("lessThan80PctDiff30", diffTo80FromAvg30)

            # current < 50% of diff
            add_if_less("lessThan50PctDiff30", diffTo50FromAvg30)

        # 90-day diff-based thresholds
        diff90 = avg90 - min90
        if diff90 is not None and diff90 > 0:
            diffTo80FromAvg90 = avg90-(diff90 * 0.8)
            diffTo50FromAvg90 = avg90-(diff90 * 0.5)
            print(f"{t}: current_price = {current_price}, diff90 = {diff90} , avg90 = {avg90}, min90 = {min90}, diffTo80FromAvg90 = {diffTo80FromAvg90}, diffTo50FromAvg90 = {diffTo50FromAvg90}")
            # current < 80% of diff
            add_if_less("lessThan80PctDiff90", diffTo80FromAvg90)

            # current < 50% of diff
            add_if_less("lessThan50PctDiff90", diffTo50FromAvg90)        

    # Insert one document per execution
    exec_state_col.insert_one(state_doc)
    log.info("Saved EveryExecutionState document at %s", create_dt.isoformat())


# ------------------- MAIN --------------------

def main():
    # 1) Make sure yesterday's data is present (if DailyLog says we haven't done today's run yet)
    ensure_yesterday_data_if_needed()

    # 2) Run analysis on whatever is in PriceFor5MinuteInterval
    analyze_and_store_execution_state()


if __name__ == "__main__":
    main()
