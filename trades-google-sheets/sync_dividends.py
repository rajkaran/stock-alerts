"""
sync_dividends.py
------------------
Incrementally syncs MongoDB Dividend records to a Google Sheet
("Dividends" tab, same spreadsheet as sync_trades.py).

- Reads  DailyLog { _id: "dividend-googlesheet" }.lastUpdateDatetime as the cursor
- Fetches Dividend records where createDatetime > cursor  (oldest-first)
- Resolves each dividend's brokerAccountId against the BrokerAccount collection
  to pull in `broker` and `accountName` (Ticker lookup is skipped — the
  dividend doc already carries `symbol` directly)
- Appends new rows to the sheet; auto-creates any new columns
- Updates the cursor to the createDatetime of the last synced record

Shared sheet/cursor/column helpers live in sheet_sync_common.py (also used
by sync_trades.py).

Schedule via cron — see README.md.

Usage:
    python sync_dividends.py

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

GOOGLE_SHEET_TAB = os.getenv("GOOGLE_DIVIDEND_SHEET_TAB", "Dividends")

# The DailyLog document that tracks how far we've synced
DAILY_LOG_ID = "dividend-googlesheet"

# Preferred left-to-right column order in the sheet.
# Any field NOT listed here is appended alphabetically after these.
PREFERRED_COLUMN_ORDER = [
    "_id", "tickerId", "symbol", "broker", "accountName", "amount", "reinvested",
    "payDatetime", "isEdited", "isActive", "createDatetime", "weekStarting",
]

# Add any field names here that you never want written to the sheet.
# brokerAccountId is resolved into `broker` / `accountName` below, so the
# raw ObjectId is dropped from the sheet output.
SKIP_FIELDS: set[str] = {"brokerAccountId"}

log = get_logger("sync_dividends")


# ─────────────────────────── RECORD MAPPING ────────────────────

def flatten_record(doc: dict, broker_accounts: dict[str, dict]) -> dict[str, str]:
    """Return a flat {field: str_value} dict, skipping SKIP_FIELDS.
    Adds a computed `weekStarting` field (Monday date) for every record, and
    resolves `broker` / `accountName` from the linked BrokerAccount doc.
    """
    flat = {k: serialize(v) for k, v in doc.items() if k not in SKIP_FIELDS}

    # Compute weekStarting from payDatetime if available
    pay_dt = doc.get("payDatetime")
    if isinstance(pay_dt, datetime):
        flat["weekStarting"] = monday_of_week(pay_dt)
    elif "weekStarting" not in flat:
        flat["weekStarting"] = ""

    # Resolve broker/account name from BrokerAccount lookup
    account_id = doc.get("brokerAccountId")
    account = broker_accounts.get(str(account_id)) if account_id else None
    flat["broker"] = account.get("broker", "") if account else ""
    flat["accountName"] = account.get("name", "") if account else ""

    return flat


def fetch_new_dividends(db, since: datetime) -> list[dict]:
    return list(
        db["Dividend"].find(
            {"createDatetime": {"$gt": since}},
            sort=[("createDatetime", 1)],
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
    dividends = fetch_new_dividends(db, since)
    log.info("%d new dividend(s) found.", len(dividends))
    if not dividends:
        log.info("Nothing to sync.")
        return

    # 3. Resolve broker accounts referenced by this batch
    account_ids = {d.get("brokerAccountId") for d in dividends}
    broker_accounts = fetch_broker_accounts(db, account_ids)
    log.info("Resolved %d broker account(s).", len(broker_accounts))

    # 4. Flatten all records
    flat = [flatten_record(d, broker_accounts) for d in dividends]

    # 5. All unique field names in this batch
    all_keys: set[str] = {k for rec in flat for k in rec}

    # 6. Open sheet and sync header
    ws = open_worksheet(GOOGLE_SHEET_ID, GOOGLE_SHEET_TAB, log)
    header = ensure_header(ws, get_header(ws, log), all_keys, PREFERRED_COLUMN_ORDER, log)

    # 7. Append rows
    append_rows(ws, header, flat, log)

    # 8. Advance cursor
    last_dt = dividends[-1].get("createDatetime")
    if last_dt:
        update_cursor(db, DAILY_LOG_ID, last_dt, log)
    else:
        log.warning("Last record has no createDatetime — cursor not updated.")

    log.info("Done.")


if __name__ == "__main__":
    run()