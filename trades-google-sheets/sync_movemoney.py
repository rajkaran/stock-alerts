"""
sync_movemoney.py
------------------
Incrementally syncs MongoDB MoveMoney records to a Google Sheet
("MoveMoney" tab, same spreadsheet as sync_trades.py / sync_dividends.py).

MoveMoney docs have no createDatetime — only `lastUpdateDatetime` — so that
field is used as both the sort key and the sync cursor.

- Reads  DailyLog { _id: "movemoney-googlesheet" }.lastUpdateDatetime as the cursor
- Fetches MoveMoney records where lastUpdateDatetime > cursor  (oldest-first)
- Resolves each record's brokerAccountId against the BrokerAccount collection
  to pull in `broker` and `accountName`
- Appends new rows to the sheet; auto-creates any new columns
- Updates the cursor to the lastUpdateDatetime of the last synced record

Shared sheet/cursor/column helpers live in sheet_sync_common.py (also used
by sync_trades.py and sync_dividends.py).

Schedule via cron — see README.md.

Usage:
    python sync_movemoney.py

Requirements:
    pip install pymongo gspread google-auth python-dotenv
"""

from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from pymongo import MongoClient

from sheet_sync_common import (
    GOOGLE_SHEET_ID,
    append_rows,
    ensure_header,
    fetch_broker_accounts,
    get_cursor,
    get_header,
    get_logger,
    monday_of_week,
    open_worksheet,
    serialize,
    update_cursor,
)

load_dotenv()

# ─────────────────────────── CONFIG ───────────────────────────

TZ = ZoneInfo(os.getenv("TZ", "America/Toronto"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "stockdb")

GOOGLE_SHEET_TAB = os.getenv("GOOGLE_MOVEMONEY_SHEET_TAB", "MoveMoney")

# The DailyLog document that tracks how far we've synced
DAILY_LOG_ID = "movemoney-googlesheet"

# Preferred left-to-right column order in the sheet.
# Any field NOT listed here is appended alphabetically after these.
PREFERRED_COLUMN_ORDER = [
    "_id", "broker", "accountName", "operation", "amount", "currency",
    "isActive", "lastUpdateDatetime", "weekStarting",
]

# Add any field names here that you never want written to the sheet.
# brokerAccountId is resolved into `broker` / `accountName` below, so the
# raw ObjectId is dropped from the sheet output.
SKIP_FIELDS: set[str] = {"brokerAccountId"}

log = get_logger("sync_movemoney")


# ─────────────────────────── RECORD MAPPING ────────────────────

def flatten_record(doc: dict, broker_accounts: dict[str, dict]) -> dict[str, str]:
    """Return a flat {field: str_value} dict, skipping SKIP_FIELDS.
    Adds a computed `weekStarting` field (Monday date) for every record, and
    resolves `broker` / `accountName` from the linked BrokerAccount doc.
    """
    flat = {k: serialize(v) for k, v in doc.items() if k not in SKIP_FIELDS}

    # Compute weekStarting from lastUpdateDatetime (MoveMoney has no other date field)
    last_dt = doc.get("lastUpdateDatetime")
    if isinstance(last_dt, datetime):
        flat["weekStarting"] = monday_of_week(last_dt)
    elif "weekStarting" not in flat:
        flat["weekStarting"] = ""

    # Resolve broker/account name from BrokerAccount lookup
    account_id = doc.get("brokerAccountId")
    account = broker_accounts.get(str(account_id)) if account_id else None
    flat["broker"] = account.get("broker", "") if account else ""
    flat["accountName"] = account.get("name", "") if account else ""

    return flat


def fetch_new_movemoney(db, since: datetime) -> list[dict]:
    return list(
        db["MoveMoney"].find(
            {"lastUpdateDatetime": {"$gt": since}},
            sort=[("lastUpdateDatetime", 1)],
        )
    )


# ─────────────────────────── MAIN ─────────────────────────────

def run():
    if not GOOGLE_SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID is not set in .env")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # 1. Read cursor
    since = get_cursor(db, DAILY_LOG_ID, log)

    # 2. Fetch new records
    records = fetch_new_movemoney(db, since)
    log.info("%d new move-money record(s) found.", len(records))
    if not records:
        log.info("Nothing to sync.")
        return

    # 3. Resolve broker accounts referenced by this batch
    account_ids = {r.get("brokerAccountId") for r in records}
    broker_accounts = fetch_broker_accounts(db, account_ids)
    log.info("Resolved %d broker account(s).", len(broker_accounts))

    # 4. Flatten all records
    flat = [flatten_record(r, broker_accounts) for r in records]

    # 5. All unique field names in this batch
    all_keys: set[str] = {k for rec in flat for k in rec}

    # 6. Open sheet and sync header
    ws = open_worksheet(GOOGLE_SHEET_ID, GOOGLE_SHEET_TAB, log)
    header = ensure_header(ws, get_header(ws, log), all_keys, PREFERRED_COLUMN_ORDER, log)

    # 7. Append rows
    append_rows(ws, header, flat, log)

    # 8. Advance cursor (lastUpdateDatetime doubles as the sync cursor here)
    last_dt = records[-1].get("lastUpdateDatetime")
    if last_dt:
        update_cursor(db, DAILY_LOG_ID, last_dt, log)
    else:
        log.warning("Last record has no lastUpdateDatetime — cursor not updated.")

    log.info("Done.")


if __name__ == "__main__":
    run()