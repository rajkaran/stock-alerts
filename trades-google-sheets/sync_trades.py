"""
sync_trades.py
--------------
Incrementally syncs MongoDB Trade records to a Google Sheet.

- Reads  DailyLog { _id: "trade-googlesheet" }.lastUpdateDatetime as the cursor
- Fetches Trade records where createDatetime > cursor  (oldest-first)
- Appends new rows to the sheet; auto-creates any new columns
- Updates the cursor to the createDatetime of the last synced record

Schedule via cron — see README.md.

Usage:
    python sync_trades.py

Requirements:
    pip install pymongo gspread google-auth python-dotenv
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from pymongo import MongoClient

load_dotenv()

# ─────────────────────────── CONFIG ───────────────────────────

TZ = ZoneInfo(os.getenv("TZ", "America/Toronto"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "stockdb")

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
GOOGLE_SHEET_ID  = os.getenv("GOOGLE_SHEET_ID",  "")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Trades")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# The DailyLog document that tracks how far we've synced
DAILY_LOG_ID = "trade-googlesheet"

# Preferred left-to-right column order in the sheet.
# Any field NOT listed here is appended alphabetically after these.
PREFERRED_COLUMN_ORDER = [
    "_id", "tickerId", "symbol", "rate", "quantity", "totalAmount", "brokerageFee", "broker",
    "tradeType", "profit", "isEdited", "isActive", "tradeDatetime", "createDatetime", "weekStarting"
]

# Add any field names here that you never want written to the sheet
SKIP_FIELDS: set[str] = set()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ─────────────────────────── LOGGING ──────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("sync_trades")


# ─────────────────────────── SERIALIZATION ────────────────────

def serialize(value) -> str:
    """Convert any MongoDB value to a plain string safe for Google Sheets."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    # Handles ObjectId, Decimal128, bool, int, float, str, etc.
    return str(value)

def monday_of_week(dt: datetime) -> str:
    """Return the Monday date of the week containing dt, as YYYY-MM-DD string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # weekday(): Monday=0, Sunday=6
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")

def flatten_record(doc: dict) -> dict[str, str]:
    """Return a flat {field: str_value} dict, skipping SKIP_FIELDS.
    Adds a computed `weekStarting` field (Monday date) for every record.
    """
    flat = {k: serialize(v) for k, v in doc.items() if k not in SKIP_FIELDS}
 
    # Compute weekStarting from tradeDatetime if available
    trade_dt = doc.get("tradeDatetime")
    if isinstance(trade_dt, datetime):
        flat["weekStarting"] = monday_of_week(trade_dt)
    elif "weekStarting" not in flat:
        flat["weekStarting"] = ""
 
    return flat


# ─────────────────────────── COLUMN MANAGEMENT ────────────────

def merge_columns(existing: list[str], incoming: set[str]) -> list[str]:
    """
    Return a merged column list:
      - existing columns keep their position
      - new columns are inserted following PREFERRED_COLUMN_ORDER,
        or appended alphabetically at the end if not in the preferred list
    """
    result = list(existing)
    truly_new = sorted(incoming - set(result))

    for col in PREFERRED_COLUMN_ORDER:
        if col not in truly_new:
            continue
        # Insert after the last preferred column already present
        anchors = [result.index(p) for p in PREFERRED_COLUMN_ORDER if p in result]
        insert_at = (max(anchors) + 1) if anchors else 0
        result.insert(insert_at, col)
        truly_new.remove(col)

    result.extend(truly_new)   # unknown new fields go to the far right
    return result


# ─────────────────────────── GOOGLE SHEETS ────────────────────

def open_worksheet(sheet_id: str, tab_name: str) -> gspread.Worksheet:
    creds = Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(sheet_id)
    try:
        ws = spreadsheet.worksheet(tab_name)
        log.info("Opened tab '%s'.", tab_name)
    except gspread.WorksheetNotFound:
        log.info("Tab '%s' not found — creating it.", tab_name)
        ws = spreadsheet.add_worksheet(title=tab_name, rows=1, cols=1)
    return ws


def get_header(ws: gspread.Worksheet) -> list[str]:
    try:
        return [c for c in ws.row_values(1) if c]
    except Exception:
        return []


def ensure_header(ws: gspread.Worksheet, current: list[str], needed: set[str]) -> list[str]:
    """Extend the header row with any missing columns and return the updated list."""
    updated = merge_columns(current, needed)
    if updated != current:
        added = [c for c in updated if c not in current]
        log.info("New column(s) added to sheet: %s", added)
        ws.update("A1", [updated])
    return updated


def append_rows(ws: gspread.Worksheet, header: list[str], records: list[dict[str, str]]):
    rows = [[rec.get(col, "") for col in header] for rec in records]
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    log.info("Appended %d row(s).", len(rows))


# ─────────────────────────── MONGODB ──────────────────────────

def get_cursor(db) -> datetime:
    """Read lastUpdateDatetime from DailyLog for _id = DAILY_LOG_ID."""
    doc = db["DailyLog"].find_one({"_id": DAILY_LOG_ID})
    if doc and "lastUpdateDatetime" in doc:
        dt = doc["lastUpdateDatetime"]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        log.info("Cursor: %s", dt.isoformat())
        return dt
    log.warning("DailyLog '%s' not found — syncing all records.", DAILY_LOG_ID)
    return datetime.min.replace(tzinfo=timezone.utc)


def update_cursor(db, dt: datetime):
    """Write lastUpdateDatetime back to DailyLog."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    db["DailyLog"].update_one(
        {"_id": DAILY_LOG_ID},
        {"$set": {"lastUpdateDatetime": dt}},
        upsert=True,
    )
    log.info("Cursor updated to %s", dt.isoformat())


def fetch_new_trades(db, since: datetime) -> list[dict]:
    return list(
        db["Trade"].find(
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
    since = get_cursor(db)

    # 2. Fetch new records
    trades = fetch_new_trades(db, since)
    log.info("%d new trade(s) found.", len(trades))
    if not trades:
        log.info("Nothing to sync.")
        return

    # 3. Flatten all records
    flat = [flatten_record(t) for t in trades]

    # 4. All unique field names in this batch
    all_keys: set[str] = {k for rec in flat for k in rec}

    # 5. Open sheet and sync header
    ws = open_worksheet(GOOGLE_SHEET_ID, GOOGLE_SHEET_TAB)
    header = ensure_header(ws, get_header(ws), all_keys)

    # 6. Append rows
    append_rows(ws, header, flat)

    # 7. Advance cursor
    last_dt = trades[-1].get("createDatetime")
    if last_dt:
        update_cursor(db, last_dt)
    else:
        log.warning("Last record has no createDatetime — cursor not updated.")

    log.info("Done.")


if __name__ == "__main__":
    run()