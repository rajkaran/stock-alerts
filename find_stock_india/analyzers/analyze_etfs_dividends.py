#!/usr/bin/env python3
"""
analyze_etfs_dividends.py

Scan ExchangeTradedFunds in Mongo where yahoo_ticker exists,
and find ETFs that have been paying dividends consistently for the
last 10 years or for every year since inception (if younger than 10 years).

Criteria:
- Use yfinance.Ticker(ticker).dividends (Series indexed by date)
- For each ETF, consider dates from max(first_dividend_date, today - 10 years)
- Require at least ONE dividend in every calendar year between start_year and current year.

Writes a boolean flag `has_consistent_dividends_10y` back to Mongo.
"""

import logging
import time
from typing import List

import pandas as pd
import yfinance as yf

from find_stock_india.common.mongo_utils import col  # <-- use your helper

# ---------------------- CONFIG ---------------------- #

ETF_COLLECTION = "ExchangeTradedFunds"

# How many years of dividend history to enforce
YEARS_REQUIRED = 10

# Whether to write a flag back into Mongo
WRITE_BACK_TO_MONGO = True
FLAG_FIELD_NAME = "has_consistent_dividends_10y"

# Throttle between yfinance calls (seconds)
YFINANCE_SLEEP_SECONDS = 0.3


# ---------------------- LOGGING ---------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------- CORE LOGIC ---------------------- #

def has_consistent_dividends(dividends: pd.Series, years_required: int = YEARS_REQUIRED) -> bool:
    """
    Check if an ETF's dividend series has at least one dividend per year for
    the last `years_required` years, OR for every year since inception if
    the ETF is younger than `years_required`.

    dividends: pandas Series indexed by DatetimeIndex, values are dividend amounts.
    """
    if dividends is None or dividends.empty:
        return False

    dividends = dividends.copy()    
    dividends.index = pd.to_datetime(dividends.index)

    # --- Force everything to Asia/Kolkata (UTC+05:30) ---
    tz_name = "Asia/Kolkata"

    if dividends.index.tz is None:
        # naive -> localize to IST
        dividends.index = dividends.index.tz_localize(tz_name)
    else:
        # already tz-aware -> convert to IST
        dividends.index = dividends.index.tz_convert(tz_name)

    # `today` and `cutoff` also in IST
    today = pd.Timestamp.now(tz=tz_name).normalize()
    cutoff = today - pd.DateOffset(years=years_required)

    first_div_date = dividends.index.min()
    # # now both `cutoff` and `first_div_date` are tz-naive, so this is safe
    start_date = max(cutoff, first_div_date)

    # Filter to the period we care about
    dividends = dividends[dividends.index >= start_date]

    if dividends.empty:
        return False

    start_year = start_date.year
    end_year = today.year

    years_with_divs = set(dividends.index.year)

    for year in range(start_year, end_year + 1):
        if year not in years_with_divs:
            return False

    return True


def process_etf_ticker(ticker: str) -> bool:
    """
    Fetch dividends for a single ETF via yfinance and return True/False
    according to the `has_consistent_dividends` rule.
    """
    try:
        yf_ticker = yf.Ticker(ticker)
        dividends = yf_ticker.dividends  # pandas Series
    except Exception as e:
        logger.warning("Error fetching data for %s: %s", ticker, e)
        return False

    if dividends is None or dividends.empty:
        logger.debug("No dividends for %s", ticker)
        return False

    return has_consistent_dividends(dividends, YEARS_REQUIRED)


def main():
    collection = col(ETF_COLLECTION)

    query = {
        "yahoo_ticker": {
            "$exists": True,
            "$ne": None,
            "$ne": ""
        }
    }

    total = collection.count_documents(query)
    logger.info("Found %d ETFs with yahoo_ticker", total)

    cursor = collection.find(query, {"_id": 1, "yahoo_ticker": 1})

    matching_tickers: List[str] = []
    processed = 0

    for doc in cursor:
        processed += 1
        ticker = doc.get("yahoo_ticker")
        etf_id = doc.get("_id")

        if not ticker:
            continue

        logger.info("(%d/%d) Checking %s", processed, total, ticker)

        is_consistent = process_etf_ticker(ticker)

        if is_consistent:
            matching_tickers.append(ticker)
            logger.info("=> %s HAS consistent dividends (10y / since inception)", ticker)
        else:
            logger.info("=> %s does NOT have consistent dividends", ticker)

        if WRITE_BACK_TO_MONGO:
            try:
                collection.update_one(
                    {"_id": etf_id},
                    {"$set": {FLAG_FIELD_NAME: is_consistent}}
                )
            except Exception as e:
                logger.warning("Mongo update failed for %s (%s): %s", ticker, etf_id, e)

        time.sleep(YFINANCE_SLEEP_SECONDS)

    logger.info("Done. %d ETFs have consistent dividends.", len(matching_tickers))
    logger.info("Matching tickers: %s", ", ".join(matching_tickers))


if __name__ == "__main__":
    main()
