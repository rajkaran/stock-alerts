"""
update_tickers_from_name.py

Generic Yahoo Finance ticker resolver for Mongo collections.

Reads:
  - Mongo collection (e.g. ExchangeTradedFunds, IndexFunds, Stocks)
  - A 'name' field (configurable via --name-field)

For each document:
  - Calls Yahoo Finance SEARCH API with the name.
  - Picks a best match (prefers Indian exchanges NSI/BSE).
  - Writes back:
      yahoo_ticker
      yahoo_exchange_raw   (e.g. 'NSI')
      yahoo_exchange       (normalized, e.g. 'NSE')
      yahoo_quote_type
      yahoo_shortname
      yahoo_longname

This script OVERWRITES these fields on every run.
You can run it once or periodically if your universe changes.

Usage examples (from repo root):
    python -m find_stock_india.universe_fetch.update_tickers_from_name \
        --collection ExchangeTradedFunds

    python -m find_stock_india.universe_fetch.update_tickers_from_name \
        --collection Stocks --name-field Name
"""

import argparse
import time
from typing import Optional, Dict, List

import requests
from pymongo import UpdateOne

from find_stock_india.common.mongo_utils import col

YF_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome Safari",
    "Accept": "application/json,text/plain,*/*",
}


def normalize_exchange(exch: Optional[str]) -> Optional[str]:
    if not exch:
        return None
    exch = exch.upper()
    # Yahoo uses 'NSI' for NSE, 'BSE' for BSE
    if exch == "NSI":
        return "NSE"
    if exch == "BSE":
        return "BSE"
    return exch  # for everything else, just keep what Yahoo says


def search_yahoo(name: str) -> Optional[Dict]:
    """
    Call Yahoo Finance search API with a security name.
    Returns the chosen quote dict or None.

    Preference:
      - First result whose exchange is NSI or BSE (Indian)
      - Otherwise the first result.
    """
    if not name:
        return None

    params = {
        "q": name,
        "quotesCount": 10,
        "newsCount": 0,
        "listsCount": 0,
        "quotesQueryId": "tss_match_phrase_query",
        "multiQuoteQueryId": "multi_quote_single_token_query",
    }

    try:
        resp = requests.get(YF_SEARCH_URL, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None

    quotes = data.get("quotes", [])
    if not quotes:
        return None

    indian = [q for q in quotes if q.get("exchange") in ("NSI", "BSE")]
    chosen = indian[0] if indian else quotes[0]
    return chosen


def update_collection_with_yahoo(
    collection_name: str,
    name_field: str = "name",
    ticker_field: str = "yahoo_ticker",
    sleep_sec: float = 0.25,
    limit: int = 0,
):
    collection = col(collection_name)

    # query = {
    #     name_field: {"$exists": True, "$ne": None},
    # }
    query = {         
        '$and':[
            {ticker_field: {'$exists': False}}, 
            {name_field: {'$exists': True}}
        ]       
    }
    print(f"[{collection_name}] querying documents missing '{query}'...")
    projection = {name_field: 1}

    cursor = collection.find(query, projection)
    if limit and limit > 0:
        cursor = cursor.limit(limit)

    

    ops: List[UpdateOne] = []
    scanned = 0
    matched = 0
    updated = 0

    for doc in cursor:
        scanned += 1
        _id = doc["_id"]
        name_val = str(doc.get(name_field, "")).strip()
        if not name_val:
            continue

        quote = search_yahoo(name_val)
        if not quote:
            # nothing found for this name
            continue

        matched += 1

        symbol = quote.get("symbol")
        exchange_raw = quote.get("exchange")
        quote_type = quote.get("quoteType")
        shortname = quote.get("shortname")
        longname = quote.get("longname")

        update_doc = {
            ticker_field: symbol,
            "yahoo_exchange_raw": exchange_raw,
            "yahoo_exchange": normalize_exchange(exchange_raw),
            "yahoo_quote_type": quote_type,
            "yahoo_shortname": shortname,
            "yahoo_longname": longname,
        }

        ops.append(UpdateOne({"_id": _id}, {"$set": update_doc}))

        if len(ops) >= 200:
            res = collection.bulk_write(ops, ordered=False)
            updated += res.modified_count + res.upserted_count
            ops = []

        if sleep_sec > 0:
            time.sleep(sleep_sec)

    if ops:
        res = collection.bulk_write(ops, ordered=False)
        updated += res.modified_count + res.upserted_count

    print(
        f"[{collection_name}] scanned={scanned}, matched={matched}, "
        f"updated={updated}, ticker_field='{ticker_field}'"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Resolve Yahoo Finance tickers + exchanges from name and store in Mongo."
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Mongo collection name (e.g. ExchangeTradedFunds, IndexFunds, Stocks)",
    )
    parser.add_argument(
        "--name-field",
        default="name",
        help="Field containing the security name (default: 'name')",
    )
    parser.add_argument(
        "--ticker-field",
        default="yahoo_ticker",
        help="Field to store Yahoo ticker into (default: 'yahoo_ticker')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit on number of docs to process (0 = no limit)",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.25,
        help="Sleep between Yahoo requests (default: 0.25 seconds)",
    )

    args = parser.parse_args()

    update_collection_with_yahoo(
        collection_name=args.collection,
        name_field=args.name_field,
        ticker_field=args.ticker_field,
        sleep_sec=args.sleep_sec,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
